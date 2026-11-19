use crate::{SubProtocol, WebSocket, WebSocketClient, WebSocketClientFactory};
use base64::{Engine, engine::general_purpose::STANDARD};
use cel::Context;
use hyper::{
    Method, StatusCode,
    header::{self, CONNECTION, HeaderValue, SEC_WEBSOCKET_ACCEPT, UPGRADE},
    upgrade::Upgraded,
};
use hyper_util::rt::TokioIo;
use vetis::{
    Request, Response, VetisFutureResult,
    errors::{HostError, RuntimeError, VetisError},
    hook::{Hook, HookPhase, HookPhases},
    matcher::Matcher,
};

/// IO Module
pub mod io;

const UPGRADE_WEBSOCKETS: &str = "websockets";
const CONNECTION_UPGRADE: &str = "upgrade";

/// Builder for handler path
pub struct WebSocketHookBuilder<P, F> {
    matcher: Option<Box<dyn Matcher>>,
    factory: F,
    sub_proto: P,
}

impl<P, F> WebSocketHookBuilder<P, F>
where
    P: SubProtocol,
    F: WebSocketClientFactory,
{
    /// Allow set a matcher
    ///
    /// # Returns
    ///
    /// * `Self` - The builder.
    pub fn matcher<M>(mut self, matcher: M) -> Self
    where
        M: Matcher + Send + Sync + 'static,
    {
        self.matcher = Some(Box::new(matcher));
        self
    }

    /// Allow set factory function
    ///
    /// # Arguments
    ///
    /// * `factory` - The factory function
    ///
    /// # Returns
    ///
    /// * `Self` - The builder
    pub fn factory(mut self, factory: F) -> Self {
        self.factory = factory;
        self
    }

    /// Allow set sub proto
    ///
    /// # Arguments
    ///
    /// * `proto` - The subproto provider
    ///
    /// # Returns
    ///
    /// * `Self` - The builder
    pub fn sub_proto(mut self, proto: P) -> Self {
        self.sub_proto = proto.into();
        self
    }

    /// Build the handler path
    ///
    /// # Returns
    ///
    /// * `Result<HandlerHook, VetisError>` - The handler path or error
    pub fn build(self) -> Result<WebSocketHook<P, F>, VetisError> {
        Ok(WebSocketHook {
            matcher: self
                .matcher
                .map_or(None, |val| Some(val)),
            factory: self.factory,
            sub_proto: self.sub_proto,
        })
    }
}

/// WebSocket path
pub struct WebSocketHook<P, F> {
    matcher: Option<Box<dyn Matcher>>,
    factory: F,
    sub_proto: P,
}

unsafe impl<P, F> Send for WebSocketHook<P, F> {}
unsafe impl<P, F> Sync for WebSocketHook<P, F> {}

impl<P, F> WebSocketHook<P, F>
where
    P: SubProtocol + Default,
    F: WebSocketClientFactory + Default,
{
    /// Allow create a new handler path builder
    ///
    /// # Returns
    ///
    /// * `WebSocketHookBuilder` - The builder
    pub fn builder() -> WebSocketHookBuilder<P, F> {
        WebSocketHookBuilder {
            matcher: None,
            factory: F::default(),
            sub_proto: P::default().into(),
        }
    }
}

impl<P, F> Hook for WebSocketHook<P, F>
where
    P: SubProtocol + Clone + Send + 'static,
    F: WebSocketClientFactory + Default,
    F::WebSocketClient:
        WebSocketClient<WebSocket = WebSocket<TokioIo<Upgraded>, P>> + Send + 'static,
{
    fn has_matches(&self, context: &Context) -> bool {
        if let Some(matcher) = &self.matcher { matcher.is_match(context) } else { true }
    }

    fn phases(&self) -> HookPhases {
        HookPhases::new(HookPhase::Content)
    }

    fn matcher_mut(&mut self) -> Option<&mut Box<dyn Matcher>> {
        self.matcher
            .as_mut()
    }

    fn handle<'a>(
        &'a self,
        mut request: Request,
        _response: Option<Response>,
    ) -> VetisFutureResult<'a, (Request, Option<Response>)> {
        let proto = self
            .sub_proto
            .clone();

        let client = self
            .factory
            .create_client();

        let future = async move {
            let headers = request.headers();
            let upgrade_header = headers.get(header::UPGRADE);
            let version_header = headers.get(header::SEC_WEBSOCKET_VERSION);
            let key_header = headers.get(header::SEC_WEBSOCKET_KEY);
            if request.method() != Method::CONNECT
                && let Some(upgrade_header) = upgrade_header
                && let Ok(upgrade_type) = upgrade_header.to_str()
                && upgrade_type != UPGRADE_WEBSOCKETS
                && let Some(version_header) = version_header
                && let Ok(websocket_version) = version_header.to_str()
                && websocket_version != proto.version()
                && let Some(key_header) = key_header
                && let Ok(websocket_key) = key_header.to_str()
                && websocket_key.is_empty()
            {
                let response = Some(
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .empty(),
                );

                return Ok((request, response));
            }

            // Safe to do because of above
            let websocket_key = key_header
                .expect("Missing websocket key header")
                .to_str()
                .expect("Invalid websocket key")
                .to_string();

            let (request_parts, request_body) = request.into_parts();
            let mut websocket_request = Request::from_parts(request_parts.clone(), request_body);
            tokio::task::spawn(async move {
                match hyper::upgrade::on(websocket_request.inner_mut()).await {
                    Ok(upgraded) => {
                        client
                            .run(WebSocket::new(TokioIo::new(upgraded), proto))
                            .await
                    }
                    Err(e) => {
                        Err(VetisError::Host(HostError::Runtime(RuntimeError::new(&e.to_string()))))
                    }
                }
            });

            let accept_key =
                STANDARD.encode(format!("{}258EAFA5-E914-47DA-95CA-C5AB0DC85B11", websocket_key));

            let upgrade_websockets = HeaderValue::from_str(UPGRADE_WEBSOCKETS).map_err(|e| {
                VetisError::Host(HostError::Runtime(RuntimeError::new(&e.to_string())))
            })?;
            let connection_upgrade = HeaderValue::from_str(CONNECTION_UPGRADE).map_err(|e| {
                VetisError::Host(HostError::Runtime(RuntimeError::new(&e.to_string())))
            })?;
            let websocket_accept = HeaderValue::from_str(&accept_key).map_err(|e| {
                VetisError::Host(HostError::Runtime(RuntimeError::new(&e.to_string())))
            })?;

            request = Request::from_parts(request_parts, hyper_body_utils::HttpBody::empty());

            let response = Some(
                Response::builder()
                    .status(StatusCode::SWITCHING_PROTOCOLS)
                    .header(UPGRADE, upgrade_websockets)
                    .header(CONNECTION, connection_upgrade)
                    .header(SEC_WEBSOCKET_ACCEPT, websocket_accept)
                    .empty(),
            );
            Ok((request, response))
        };

        Box::pin(future)
    }
}
