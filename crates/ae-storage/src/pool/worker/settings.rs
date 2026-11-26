use super::*;
use crate::pool::PolicySettings;

impl Client {
    // Share the existing retained read-reply budget with account queries;
    // replies remain charged while queued, executing and owned by the caller.
    pub fn policy_settings(
        &self,
        maximum_bytes: usize,
    ) -> Result<Ticket<accounts::Retained<PolicySettings>>, Error> {
        let bytes = BASE_WEIGHT
            .checked_add(maximum_bytes)
            .ok_or(Error::RequestTooLarge)?;
        let credits = self.shared.accounts.reserve(bytes)?;
        self.submit(BASE_WEIGHT, move |service| {
            let value = service.read_policy_settings(maximum_bytes)?;
            Ok(accounts::Retained { value, credits })
        })
    }
}
