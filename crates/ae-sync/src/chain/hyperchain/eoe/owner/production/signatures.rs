use crate::preset_keys::Observed;
use ae_codec::headers::MicroHeader;
use ae_types::Signature;

// These receipts never change worker incarnation and never resubmit work.
// Keep them in stopped-production reports until their caller releases them.
#[derive(Debug, Clone, Default)]
pub struct SigningReceipts {
    pub candidate: Option<Observed<()>>,
    pub key: Option<Observed<Signature>>,
    pub micro: Option<Observed<MicroHeader>>,
}
impl SigningReceipts {
    pub fn poll(&mut self) {
        if let Some(receipt) = &mut self.candidate {
            receipt.poll();
        }
        if let Some(receipt) = &mut self.key {
            receipt.poll();
        }
        if let Some(receipt) = &mut self.micro {
            receipt.poll();
        }
    }
    pub fn pending(&self) -> bool {
        self.candidate
            .as_ref()
            .is_some_and(|t| t.try_result().is_none())
            || self.key.as_ref().is_some_and(|t| t.try_result().is_none())
            || self
                .micro
                .as_ref()
                .is_some_and(|t| t.try_result().is_none())
    }
}
