use super::*;

pub(crate) struct RoutedCached<P> {
    cached: Cached<P>,
    route: Option<cache::Route>,
}
impl<P> RoutedCached<P> {
    pub(crate) fn new(cached: Cached<P>, route: cache::Route) -> Self {
        Self {
            cached,
            route: Some(route),
        }
    }
}
impl<P: PinInputs + Clone> ParentInputs for RoutedCached<P> {
    type State = inputs::Effects;
    type Error = CachedError<P::Error>;
    type HostError = inputs::InputError;
    type Host<'db> = inputs::Captured<Snapshot<'db>>;
    fn bind(&self) -> Result<Option<Self>, Self::Error> {
        let Some(route) = &self.route else {
            return Ok(None);
        };
        let cache = route.current().ok_or(CachedError::Closed)?;
        let binding = Self {
            cached: Cached {
                cache,
                ..self.cached.clone()
            },
            route: None,
        };
        binding.check()?;
        Ok(Some(binding))
    }
    fn check(&self) -> Result<(), Self::Error> {
        self.cached.check()
    }
    fn capture<'db>(
        &self,
        child: &repository::View<Snapshot<'db>>,
        block: &Block,
        previous: &inputs::Effects,
    ) -> Result<Self::Host<'db>, Self::Error> {
        self.cached.capture(child, block, previous)
    }
    fn acquire<'db>(
        &self,
        host: &mut Self::Host<'db>,
        error: &repository::HyperchainError<inputs::InputError, Snapshot<'db>>,
        check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<bool, Error> {
        self.cached.acquire(host, error, check)
    }
    fn detach(host: Self::Host<'_>) -> inputs::Effects {
        host.into_effects()
    }
}
