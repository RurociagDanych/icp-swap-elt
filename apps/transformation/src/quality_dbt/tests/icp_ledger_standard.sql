-- The native ICP ledger is recorded both as ICP and as ICRC2; the merge rules make ICP win.
select ledger_id, standard
from {{ source('silver', 'token_dim') }}
where ledger_id = 'ryjl3-tyaaa-aaaaa-aaaba-cai'
  and standard is distinct from 'ICP'
