use crate::{SubProtocol, WebSocket, WebSocketClient, WebSocketClientFactory};
use base64::{Engine, engine::general_purpose::STANDARD};
use cel::Context;
use hyper::{
    Method, StatusCode,
    header::{self, CONNECTION, HeaderValue, SEC_WEBSOCKET_ACCEPT, UPGRADE},
    upgrade::Upgraded,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use vetis::{
    Request, Response, VetisFutureResult, VetisHook,
    errors::{HostError, RuntimeError, VetisError},
    hook::HookPhase,
    matcher::{Matcher, MatcherConfig},
};

/// IO Module
pub mod io;

const UPGRADE_WEBSOCKETS: &str = "websockets";
const CONNECTION_UPGRADE: &str = "upgrade";

/// Builder for websocket hook configuration
pub struct HookConfigBuilder<P, F> {
    matcher: Option<Box<dyn MatcherConfig>>,
    factory: F,
    sub_proto: P,
}

impl<P, F> HookConfigBuilder<P, F>
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
        M: MatcherConfig + Send + Sync + 'static,
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
        self.sub_proto = proto;
        self
    }

    /// Build the handler path
    ///
    /// # Returns
    ///
    /// * `Result<HandlerHook, VetisError>` - The handler path or error
    pub fn build(self) -> Result<HookConfig<P, F>, VetisError> {
        Ok(HookConfig {
            matcher: self
                .matcher
                .map_or(None, |val| Some(val)),
            factory: Some(self.factory),
            sub_proto: Some(self.sub_proto),
        })
    }
}

#[derive(Serialize, Deserialize)]
/// Hook config
pub struct HookConfig<P, F> {
    matcher: Option<Box<dyn MatcherConfig>>,
    #[serde(skip)]
    factory: Option<F>,
    #[serde(skip)]
    sub_proto: Option<P>,
}

impl<P, F> Clone for HookConfig<P, F>
where
    P: Clone + 'static,
    F: Clone + 'static,
{
    fn clone(&self) -> Self {
        HookConfig {
            matcher: self.matcher.clone(),
            factory: self.factory.clone(),
            sub_proto: self
                .sub_proto
                .clone(),
        }
    }
}

impl<P, F> Default for HookConfig<P, F> {
    fn default() -> Self {
        HookConfig { matcher: None, factory: None, sub_proto: None }
    }
}

#[typetag::serialize]
impl<P, F> vetis::hook::HookConfig for HookConfig<P, F>
where
    P: SubProtocol + Default + Clone + Send + Sync + 'static,
    F: WebSocketClientFactory + Default + Clone + 'static,
    F::WebSocketClient:
        WebSocketClient<WebSocket = WebSocket<TokioIo<Upgraded>, P>> + Send + 'static,
{
    fn matcher(&self) -> Option<&Box<dyn MatcherConfig>> {
        self.matcher
            .as_ref()
    }

    fn boxed_clone(&self) -> Box<dyn vetis::hook::HookConfig> {
        Box::new(self.clone())
    }

    fn boxed_hook<'a>(&'a self) -> Box<dyn VetisHook> {
        Box::new(WebSocketHook::new(self.clone()))
    }

    fn typetag_deserialize(&self) {
        unimplemented!()
    }
}

impl<P, F> HookConfig<P, F>
where
    P: SubProtocol + Default,
    F: WebSocketClientFactory + Default,
{
    /// Allow create a new configuration for this hook.
    ///
    /// # Returns
    ///
    /// * `HookConfigBuilder` - The builder
    pub fn builder() -> HookConfigBuilder<P, F> {
        HookConfigBuilder { matcher: None, factory: F::default(), sub_proto: P::default().into() }
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
    pub fn factory(&self) -> Option<&F> {
        self.factory
            .as_ref()
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
    pub fn sub_proto(&self) -> Option<&P> {
        self.sub_proto
            .as_ref()
    }
}

/// WebSocket path
pub struct WebSocketHook<P, F> {
    matcher: Option<Box<dyn Matcher>>,
    config: HookConfig<P, F>,
}

unsafe impl<P, F> Send for WebSocketHook<P, F> {}
unsafe impl<P, F> Sync for WebSocketHook<P, F> {}

impl<P, F> WebSocketHook<P, F>
where
    P: SubProtocol + Default,
    F: WebSocketClientFactory + Default,
{
    /// Create a new websocket hook based on its configuration
    pub fn new(config: HookConfig<P, F>) -> Self {
        Self { matcher: None, config }
    }
}

impl<P, F> vetis::hook::Hook for WebSocketHook<P, F>
where
    P: SubProtocol + Clone + Send + 'static,
    F: WebSocketClientFactory + Default,
    F::WebSocketClient:
        WebSocketClient<WebSocket = WebSocket<TokioIo<Upgraded>, P>> + Send + 'static,
{
    fn setup<'a>(&'a mut self) -> VetisFutureResult<'a, ()> {
        Box::pin(async move {
            let matcher = if let Some(matcher_config) = &self.config.matcher {
                Some(matcher_config.boxed_matcher())
            } else {
                None
            };
            self.matcher = matcher;
            Ok(())
        })
    }

    fn has_matches(&self, context: &Context) -> bool {
        if let Some(matcher) = &self.matcher { matcher.is_match(context) } else { true }
    }

    fn phase(&self) -> HookPhase {
        HookPhase::Content
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
            .config
            .sub_proto
            .as_ref()
            .expect("Invalid protocol")
            .clone();

        let client = self
            .config
            .factory
            .as_ref()
            .expect("Invalid factory")
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
