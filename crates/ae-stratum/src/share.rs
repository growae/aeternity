use crate::{
    Error, integer,
    job::Job,
    nonce::{Kind, Nonce, Part},
    proof::Proof,
};
use ae_consensus::cuckoo;
use ae_types::Hash256;
use num_bigint::{BigInt, Sign};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub account: String,
    pub worker: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Validity {
    ValidBlock,
    ValidShare,
    UserNotFound,
    InvalidMinerNonce,
    DuplicateShare,
    InvalidSolution,
    HighTargetShare,
    MaxSolveTimeExceeded,
    JobNotFound,
}
impl Validity {
    pub fn accepted(self) -> bool {
        matches!(self, Self::ValidBlock | Self::ValidShare)
    }
    pub fn response(self) -> Response {
        match self {
            Self::ValidBlock | Self::ValidShare => Response::Result(true),
            Self::MaxSolveTimeExceeded => Response::Result(false),
            Self::UserNotFound => Response::Error(Rejection::UnauthorizedWorker, None),
            Self::DuplicateShare => Response::Error(Rejection::DuplicateShare, None),
            Self::HighTargetShare => Response::Error(Rejection::LowDifficultyShare, None),
            Self::JobNotFound => Response::Error(Rejection::JobNotFound, None),
            Self::InvalidMinerNonce => {
                Response::Error(Rejection::UnknownError, Some("invalid_miner_nonce"))
            }
            Self::InvalidSolution => {
                Response::Error(Rejection::UnknownError, Some("invalid_solution"))
            }
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    UnauthorizedWorker,
    DuplicateShare,
    LowDifficultyShare,
    JobNotFound,
    UnknownError,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Response {
    Result(bool),
    Error(Rejection, Option<&'static str>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    SubmitShare {
        account: String,
        target: BigInt,
        hash: Hash256,
    },
    SubmitSolution {
        hash: Hash256,
        nonce: Nonce,
        proof: Box<[u64; 42]>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Share {
    pub user: User,
    pub miner_nonce: Part,
    pub proof: Proof,
    pub validity: Option<Validity>,
    pub created: BigInt,
}
impl Share {
    pub fn validate(&self) -> Result<(), Error> {
        if self.user.account.len() > 1024 || self.user.worker.len() > 1024 {
            return Err(Error::Limit("user"));
        }
        integer(&self.created)
    }
    pub fn retained_bytes(&self) -> usize {
        256 + self.user.account.len()
            + self.user.worker.len()
            + self.created.bits().div_ceil(8) as usize
            + self.proof.retained_bytes()
    }
    pub fn duplicates(&self, nonce: Part, proof: &Proof) -> bool {
        self.miner_nonce == nonce && self.proof.duplicates(proof)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision {
    pub validity: Validity,
    pub nonce: Option<Nonce>,
}
// Authorization is the current connection/account membership result supplied
// by the registry owner. This pure boundary does not establish that ownership.
pub struct Context<'a> {
    pub extra_nonce: Part,
    pub authorized: bool,
    pub maximum_solve_time: &'a BigInt,
    pub edge_bits: u8,
}
pub fn classify(
    job: Option<&Job>,
    share: &Share,
    context: &Context<'_>,
) -> Result<Decision, Error> {
    share.validate()?;
    integer(context.maximum_solve_time)?;
    if context.extra_nonce.kind() != Kind::Extra || !(1..=63).contains(&context.edge_bits) {
        return Err(Error::Configuration);
    }
    let rejected = |validity| {
        Ok(Decision {
            validity,
            nonce: None,
        })
    };
    let Some(job) = job else {
        return rejected(Validity::JobNotFound);
    };
    if !context.authorized {
        return rejected(Validity::UserNotFound);
    }
    if share.miner_nonce.bytes() != context.extra_nonce.complement() {
        return rejected(Validity::InvalidMinerNonce);
    }
    if job.contains(share.miner_nonce, &share.proof) {
        return rejected(Validity::DuplicateShare);
    }
    let nonce = context.extra_nonce.merge(share.miner_nonce)?;
    let Some(proof) = share.proof.checked_edges(context.edge_bits)? else {
        return rejected(Validity::InvalidSolution);
    };
    if !cuckoo::verify_proof_from_hash(job.work().hash(), nonce.0, &proof, context.edge_bits) {
        return rejected(Validity::InvalidSolution);
    }
    let target = BigInt::from_bytes_be(
        Sign::Plus,
        &cuckoo::solution_hash(&proof, context.edge_bits)
            .ok_or(Error::Configuration)?
            .0,
    );
    // Pool share classification is inclusive; ordinary consensus admission
    // retains its stricter target test and independently validates the block.
    let validity = if target <= job.work().block_target {
        Validity::ValidBlock
    } else if target <= job.share_target() {
        Validity::ValidShare
    } else {
        return rejected(Validity::HighTargetShare);
    };
    if &share.created - job.created() > *context.maximum_solve_time {
        return rejected(Validity::MaxSolveTimeExceeded);
    }
    Ok(Decision {
        validity,
        nonce: Some(nonce),
    })
}
