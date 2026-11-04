"""Dataset A/B seam reconciliation tests, using real sanitized data.

Fixtures below are genuine rows sliced from the real files involved in the
Dataset A/B seam: Dataset-A canister file
0071_3326b-qiaaa-aaaag-qnlja-cai.csv (its real chronological tail) and Dataset-B
transactions_backfill_..._p001001.csv (the one real backfill file overlapping that
tail by ~6.25h). Principal IDs/pool IDs/amounts are real on-chain public values;
nothing sensitive is embedded. A_FIXTURE holds 10 real Dataset-A rows inside the
real overlap window; B_FIXTURE holds the 10 real Dataset-B rows that genuinely
duplicate them (content-identical, exact 0-second timestamp match) plus one real
Dataset-B row in the same window with NO Dataset-A match,
and one real Dataset-B row entirely outside the window (must survive untouched).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from seam_reconciliation import (  # noqa: E402
    clean_dataset_b_overlap,
    dataset_a_window_index,
)

WINDOW_START = datetime(2026, 6, 7, 0, 22, 33, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 6, 7, 6, 37, 26, tzinfo=timezone.utc)

A_HEADER = (
    "storage_canister,hash,timestamp,action,pool_id,sender,from,to,recipient,"
    "token0_id,token0_symbol,token0_standard,token0_change,token0_price,token0_decimals,"
    "token1_id,token1_symbol,token1_standard,token1_change,token1_price,token1_decimals,"
    "amount_usd,amount_token0,amount_token1,pool_fee,tick,liquidity_total,liquidity_change"
)
A_ROWS = [
    "3326b-qiaaa-aaaag-qnlja-cai,fqqdu-3qaaa-aaaar-qb4oa-cai1935,1780814246,swap,fqqdu-3qaaa-aaaar-qb4oa-cai,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,fqqdu-3qaaa-aaaar-qb4oa-cai,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,mxzaz-hqaaa-aaaar-qaada-cai,ckBTC,ICRC2,0.00007635,60253.5224213743,8,4zmwe-paaaa-aaaam-qdwxa-cai,dvinity,ICRC2,41224.29803415,0.0001115933237495279,8,4.600356436871928,0.00007635,41224.29803415,3000,0,0,0",
    "3326b-qiaaa-aaaag-qnlja-cai,bzxyy-5qaaa-aaaar-qb3jq-cai4636,1780814233,swap,bzxyy-5qaaa-aaaar-qb3jq-cai,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,bzxyy-5qaaa-aaaar-qb3jq-cai,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,cngnf-vqaaa-aaaar-qag4q-cai,ckUSDT,ICRC2,46.604244,0.999950004,6,ly36x-wiaaa-aaaai-aqj7q-cai,VCHF,ICRC2,37.01022375,1.2591632595632978,8,46.60191397421698,46.604244,37.01022375,3000,0,0,0",
    "3326b-qiaaa-aaaag-qnlja-cai,osyzs-xiaaa-aaaag-qc76q-cai63481,1780813470,swap,osyzs-xiaaa-aaaag-qc76q-cai,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,osyzs-xiaaa-aaaag-qc76q-cai,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,hvgxa-wqaaa-aaaaq-aacia-cai,SNEED,ICRC2,0.03255347,30.77034149966415,8,ryjl3-tyaaa-aaaaa-aaaba-cai,ICP,ICRC2,0.43494688,2.3029970669040596,8,1.0016813888990719,0.03255347,0.43494688,3000,0,0,0",
    "3326b-qiaaa-aaaag-qnlja-cai,osyzs-xiaaa-aaaag-qc76q-cai63479,1780812967,swap,osyzs-xiaaa-aaaag-qc76q-cai,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,osyzs-xiaaa-aaaag-qc76q-cai,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,hvgxa-wqaaa-aaaaq-aacia-cai,SNEED,ICRC2,0.03430535,30.771883735251308,8,ryjl3-tyaaa-aaaaa-aaaba-cai,ICP,ICRC2,0.45837672,2.3029970669040596,8,1.0556402416971034,0.03430535,0.45837672,3000,0,0,0",
    "3326b-qiaaa-aaaag-qnlja-cai,osyzs-xiaaa-aaaag-qc76q-cai63478,1780812943,swap,osyzs-xiaaa-aaaag-qc76q-cai,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,osyzs-xiaaa-aaaag-qc76q-cai,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,hvgxa-wqaaa-aaaaq-aacia-cai,SNEED,ICRC2,0.07016583,30.774287574766877,8,ryjl3-tyaaa-aaaaa-aaaba-cai,ICP,ICRC2,0.93760581,2.3029970669040596,8,2.159303430342205,0.07016583,0.93760581,3000,0,0,0",
    "3326b-qiaaa-aaaag-qnlja-cai,fqqdu-3qaaa-aaaar-qb4oa-cai1934,1780812711,swap,fqqdu-3qaaa-aaaar-qb4oa-cai,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,fqqdu-3qaaa-aaaar-qb4oa-cai,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,mxzaz-hqaaa-aaaar-qaada-cai,ckBTC,ICRC2,0.000062,60253.5224213743,8,4zmwe-paaaa-aaaam-qdwxa-cai,dvinity,ICRC2,33816.90820083,0.0001104689514470018,8,3.7357183901252067,0.000062,33816.90820083,3000,0,0,0",
    "3326b-qiaaa-aaaag-qnlja-cai,osyzs-xiaaa-aaaag-qc76q-cai63469,1780809221,swap,osyzs-xiaaa-aaaag-qc76q-cai,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,osyzs-xiaaa-aaaag-qc76q-cai,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,hvgxa-wqaaa-aaaaq-aacia-cai,SNEED,ICRC2,0.07163579,30.777085622433095,8,ryjl3-tyaaa-aaaaa-aaaba-cai,ICP,ICRC2,0.95734986,2.302962516190932,8,2.2047408424606365,0.07163579,0.95734986,3000,0,0,0",
    "3326b-qiaaa-aaaag-qnlja-cai,fqqdu-3qaaa-aaaar-qb4oa-cai1933,1780809197,swap,fqqdu-3qaaa-aaaar-qb4oa-cai,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,fqqdu-3qaaa-aaaar-qb4oa-cai,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,mxzaz-hqaaa-aaaar-qaada-cai,ckBTC,ICRC2,0.00007205,60252.61846791381,8,4zmwe-paaaa-aaaam-qdwxa-cai,dvinity,ICRC2,39690.60336436,0.00010937604351238843,8,4.34120116061319,0.00007205,39690.60336436,3000,0,0,0",
    "3326b-qiaaa-aaaag-qnlja-cai,osyzs-xiaaa-aaaag-qc76q-cai63464,1780807710,swap,osyzs-xiaaa-aaaag-qc76q-cai,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,osyzs-xiaaa-aaaag-qc76q-cai,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,hvgxa-wqaaa-aaaaq-aacia-cai,SNEED,ICRC2,0.10218151,30.783697652010552,8,ryjl3-tyaaa-aaaaa-aaaba-cai,ICP,ICRC2,1.36574405,2.3031582743969436,8,3.1455247094658927,0.10218151,1.36574405,3000,0,0,0",
    "3326b-qiaaa-aaaag-qnlja-cai,osyzs-xiaaa-aaaag-qc76q-cai63463,1780807458,swap,osyzs-xiaaa-aaaag-qc76q-cai,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,osyzs-xiaaa-aaaag-qc76q-cai,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,hvgxa-wqaaa-aaaaq-aacia-cai,SNEED,ICRC2,0.03810383,30.786918339723222,8,ryjl3-tyaaa-aaaaa-aaaba-cai,ICP,ICRC2,0.50934385,2.3031582743969436,8,1.1730995026406958,0.03810383,0.50934385,3000,0,0,0",
]
A_CSV = (A_HEADER + "\r\n" + "\r\n".join(A_ROWS) + "\r\n").encode("utf-8")

B_HEADER = (
    "tx_hash,tx_time,action_type,pool_id,pool_fee,position_id,from_principal_id,"
    "from_subaccount,from_account_id,from_textual_id,from_alias,to_principal_id,"
    "to_subaccount,to_account_id,to_textual_id,to_alias,token0_ledger_id,token0_name,"
    "token0_symbol,token0_price,token1_ledger_id,token1_name,token1_symbol,token1_price,"
    "token0_amount_in,token1_amount_in,token0_amount_out,token1_amount_out,token0_fee,"
    "token1_fee,token0_tx_value,token1_tx_value,sqrt_price,tick_limit,tick,liquidity,"
    "current_liquidity"
)
# First 10 rows genuinely duplicate the 10 A_ROWS above (content-identical real swaps,
# exact 0-second timestamp match). Row 11 is a real B row in-window with NO A match
# (must survive). Row 12 is a real B row entirely outside the window (must survive).
B_ROWS = [
    "fqqdu-3qaaa-aaaar-qb4oa-cai_fqqdu-3qaaa-aaaar-qb4oa-cai1935,1780814246000,Swap,fqqdu-3qaaa-aaaar-qb4oa-cai,3000,0,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,0000000000000000000000000000000000000000000000000000000000000000,4e18c7452ec8a54c87c3751b6e93ab2b26d5ada04ecdacc63452b4b945c5e580,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,,fqqdu-3qaaa-aaaar-qb4oa-cai,0000000000000000000000000000000000000000000000000000000000000000,7136b337b901ad457e3d52c095c5d2acf0092c1eaedabf4fc2e896174ee1d3f2,fqqdu-3qaaa-aaaar-qb4oa-cai,ICPSwap:dvinity/ckBTC,mxzaz-hqaaa-aaaar-qaada-cai,ckBTC,ckBTC,60253.5224213743,4zmwe-paaaa-aaaam-qdwxa-cai,dvinity,dvinity,0.000111593323749528,0.00007635,0,0,41224.29803415,0.0000001,0.01,4.60035644,4.60035644,,,0,0,0",
    "bzxyy-5qaaa-aaaar-qb3jq-cai_bzxyy-5qaaa-aaaar-qb3jq-cai4636,1780814233000,Swap,bzxyy-5qaaa-aaaar-qb3jq-cai,3000,0,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,0000000000000000000000000000000000000000000000000000000000000000,4e18c7452ec8a54c87c3751b6e93ab2b26d5ada04ecdacc63452b4b945c5e580,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,,bzxyy-5qaaa-aaaar-qb3jq-cai,0000000000000000000000000000000000000000000000000000000000000000,2abdd3b2502e8a69dab9e9e2e15a22cd575149243cdfc244f152d33f7df03f18,bzxyy-5qaaa-aaaar-qb3jq-cai,ICPSwap:ckUSDT/VCHF,cngnf-vqaaa-aaaar-qag4q-cai,ckUSDT,ckUSDT,0.999950004,ly36x-wiaaa-aaaai-aqj7q-cai,VNX Franc,VCHF,1.2591632595632978,46.604244,0,0,37.01022375,0.01,0.0001,46.60191397,46.60191397,,,0,0,0",
    "osyzs-xiaaa-aaaag-qc76q-cai_osyzs-xiaaa-aaaag-qc76q-cai63481,1780813470000,Swap,osyzs-xiaaa-aaaag-qc76q-cai,3000,0,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,0000000000000000000000000000000000000000000000000000000000000000,6d123e9bc79ea9f2ced187b8c58338341ef49cacb02792bbdb37ef455d27da74,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,,osyzs-xiaaa-aaaag-qc76q-cai,0000000000000000000000000000000000000000000000000000000000000000,a01e929224cfb144a85240fc1419e17e1f6d106f21dda6c6be7c33116adcbf35,osyzs-xiaaa-aaaag-qc76q-cai,ICPSwap:SNEED/ICP,hvgxa-wqaaa-aaaaq-aacia-cai,Sneed DAO,SNEED,30.77034149966415,ryjl3-tyaaa-aaaaa-aaaba-cai,Internet Computer,ICP,2.3029970669040596,0.03255347,0,0,0.43494688,0,0,1.00168139,1.00168139,,,0,0,0",
    "osyzs-xiaaa-aaaag-qc76q-cai_osyzs-xiaaa-aaaag-qc76q-cai63479,1780812967000,Swap,osyzs-xiaaa-aaaag-qc76q-cai,3000,0,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,0000000000000000000000000000000000000000000000000000000000000000,1ef5392a3496f47bd41277aab184151cb02f3ca9c6e8930a414740dad8259cd9,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,,osyzs-xiaaa-aaaag-qc76q-cai,0000000000000000000000000000000000000000000000000000000000000000,a01e929224cfb144a85240fc1419e17e1f6d106f21dda6c6be7c33116adcbf35,osyzs-xiaaa-aaaag-qc76q-cai,ICPSwap:SNEED/ICP,hvgxa-wqaaa-aaaaq-aacia-cai,Sneed DAO,SNEED,30.771883735251308,ryjl3-tyaaa-aaaaa-aaaba-cai,Internet Computer,ICP,2.3029970669040596,0.03430535,0,0,0.45837672,0.00001,0.0001,1.05564024,1.05564024,,,0,0,0",
    "osyzs-xiaaa-aaaag-qc76q-cai_osyzs-xiaaa-aaaag-qc76q-cai63478,1780812943000,Swap,osyzs-xiaaa-aaaag-qc76q-cai,3000,0,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,0000000000000000000000000000000000000000000000000000000000000000,1ef5392a3496f47bd41277aab184151cb02f3ca9c6e8930a414740dad8259cd9,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,,osyzs-xiaaa-aaaag-qc76q-cai,0000000000000000000000000000000000000000000000000000000000000000,a01e929224cfb144a85240fc1419e17e1f6d106f21dda6c6be7c33116adcbf35,osyzs-xiaaa-aaaag-qc76q-cai,ICPSwap:SNEED/ICP,hvgxa-wqaaa-aaaaq-aacia-cai,Sneed DAO,SNEED,30.774287574766877,ryjl3-tyaaa-aaaaa-aaaba-cai,Internet Computer,ICP,2.3029970669040596,0.07016583,0,0,0.93760581,0.00001,0.0001,2.15930343,2.15930343,,,0,0,0",
    "fqqdu-3qaaa-aaaar-qb4oa-cai_fqqdu-3qaaa-aaaar-qb4oa-cai1934,1780812711000,Swap,fqqdu-3qaaa-aaaar-qb4oa-cai,3000,0,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,0000000000000000000000000000000000000000000000000000000000000000,4e18c7452ec8a54c87c3751b6e93ab2b26d5ada04ecdacc63452b4b945c5e580,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,,fqqdu-3qaaa-aaaar-qb4oa-cai,0000000000000000000000000000000000000000000000000000000000000000,7136b337b901ad457e3d52c095c5d2acf0092c1eaedabf4fc2e896174ee1d3f2,fqqdu-3qaaa-aaaar-qb4oa-cai,ICPSwap:dvinity/ckBTC,mxzaz-hqaaa-aaaar-qaada-cai,ckBTC,ckBTC,60253.5224213743,4zmwe-paaaa-aaaam-qdwxa-cai,dvinity,dvinity,0.000110468951447002,0.000062,0,0,33816.90820083,0.0000001,0.01,3.73571839,3.73571839,,,0,0,0",
    "osyzs-xiaaa-aaaag-qc76q-cai_osyzs-xiaaa-aaaag-qc76q-cai63469,1780809221000,Swap,osyzs-xiaaa-aaaag-qc76q-cai,3000,0,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,0000000000000000000000000000000000000000000000000000000000000000,1ef5392a3496f47bd41277aab184151cb02f3ca9c6e8930a414740dad8259cd9,tg3k5-6b7d3-afr4z-jrcyn-faouq-mxhyu-zx46x-pflml-dzptp-3owaw-dqe,,osyzs-xiaaa-aaaag-qc76q-cai,0000000000000000000000000000000000000000000000000000000000000000,a01e929224cfb144a85240fc1419e17e1f6d106f21dda6c6be7c33116adcbf35,osyzs-xiaaa-aaaag-qc76q-cai,ICPSwap:SNEED/ICP,hvgxa-wqaaa-aaaaq-aacia-cai,Sneed DAO,SNEED,30.777085622433095,ryjl3-tyaaa-aaaaa-aaaba-cai,Internet Computer,ICP,2.302962516190932,0.07163579,0,0,0.95734986,0.00001,0.0001,2.20474084,2.20474084,,,0,0,0",
    "fqqdu-3qaaa-aaaar-qb4oa-cai_fqqdu-3qaaa-aaaar-qb4oa-cai1933,1780809197000,Swap,fqqdu-3qaaa-aaaar-qb4oa-cai,3000,0,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,0000000000000000000000000000000000000000000000000000000000000000,6d123e9bc79ea9f2ced187b8c58338341ef49cacb02792bbdb37ef455d27da74,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,,fqqdu-3qaaa-aaaar-qb4oa-cai,0000000000000000000000000000000000000000000000000000000000000000,7136b337b901ad457e3d52c095c5d2acf0092c1eaedabf4fc2e896174ee1d3f2,fqqdu-3qaaa-aaaar-qb4oa-cai,ICPSwap:dvinity/ckBTC,mxzaz-hqaaa-aaaar-qaada-cai,ckBTC,ckBTC,60252.61846791381,4zmwe-paaaa-aaaam-qdwxa-cai,dvinity,dvinity,0.000109376043512388,0.00007205,0,0,39690.60336436,0.0000001,0.01,4.34120116,4.34120116,,,0,0,0",
    "osyzs-xiaaa-aaaag-qc76q-cai_osyzs-xiaaa-aaaag-qc76q-cai63464,1780807710000,Swap,osyzs-xiaaa-aaaag-qc76q-cai,3000,0,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,0000000000000000000000000000000000000000000000000000000000000000,6d123e9bc79ea9f2ced187b8c58338341ef49cacb02792bbdb37ef455d27da74,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,,osyzs-xiaaa-aaaag-qc76q-cai,0000000000000000000000000000000000000000000000000000000000000000,a01e929224cfb144a85240fc1419e17e1f6d106f21dda6c6be7c33116adcbf35,osyzs-xiaaa-aaaag-qc76q-cai,ICPSwap:SNEED/ICP,hvgxa-wqaaa-aaaaq-aacia-cai,Sneed DAO,SNEED,30.783697652010552,ryjl3-tyaaa-aaaaa-aaaba-cai,Internet Computer,ICP,2.3031582743969436,0.10218151,0,0,1.36574405,0,0,3.14552471,3.14552471,,,0,0,0",
    "osyzs-xiaaa-aaaag-qc76q-cai_osyzs-xiaaa-aaaag-qc76q-cai63463,1780807458000,Swap,osyzs-xiaaa-aaaag-qc76q-cai,3000,0,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,0000000000000000000000000000000000000000000000000000000000000000,6d123e9bc79ea9f2ced187b8c58338341ef49cacb02792bbdb37ef455d27da74,svggt-dmjvr-ucmjh-z7s3t-eivnj-hhe3g-vq6do-jx7ty-72lac-w7lfx-kae,,osyzs-xiaaa-aaaag-qc76q-cai,0000000000000000000000000000000000000000000000000000000000000000,a01e929224cfb144a85240fc1419e17e1f6d106f21dda6c6be7c33116adcbf35,osyzs-xiaaa-aaaag-qc76q-cai,ICPSwap:SNEED/ICP,hvgxa-wqaaa-aaaaq-aacia-cai,Sneed DAO,SNEED,30.786918339723222,ryjl3-tyaaa-aaaaa-aaaba-cai,Internet Computer,ICP,2.3031582743969436,0.03810383,0,0,0.50934385,0,0,1.1730995,1.1730995,,,0,0,0",
]
B_UNIQUE_IN_WINDOW_ROW = "xmiu5-jqaaa-aaaag-qbz7q-cai488819,1780814246000,Swap,xmiu5-jqaaa-aaaag-qbz7q-cai,3000,0,ebvuq-73zru-injn5-gm37o-xg7ki-6r5bc-sge2s-3gdb7-r2f5n-ajvev-iae,0000000000000000000000000000000000000000000000000000000000000000,44314be6f019563665a0b858531b02b3ca3547db142f320b1eead496933ac0a9,ebvuq-73zru-injn5-gm37o-xg7ki-6r5bc-sge2s-3gdb7-r2f5n-ajvev-iae,,xmiu5-jqaaa-aaaag-qbz7q-cai,0000000000000000000000000000000000000000000000000000000000000000,178197f9833164374be1e0ff8e9cf8b78c964f3ea294ab0da9bddc800c7ac64f,xmiu5-jqaaa-aaaag-qbz7q-cai,ICPSwap:ckBTC/ICP,mxzaz-hqaaa-aaaar-qaada-cai,ckBTC,ckBTC,61775.5438789468537966,ryjl3-tyaaa-aaaaa-aaaba-cai,Internet Computer,ICP,2.448412535116226,0.00072586,0,0,18.31406907,0,0,44.84039628,44.84039628,126024...0000,,101391,0,56605821459"
B_OUT_OF_WINDOW_ROW = "mohjv-bqaaa-aaaag-qjyia-cai875344,1780858942000,Swap,mohjv-bqaaa-aaaag-qjyia-cai,3000,0,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,0000000000000000000000000000000000000000000000000000000000000000,4e18c7452ec8a54c87c3751b6e93ab2b26d5ada04ecdacc63452b4b945c5e580,ehq2s-mlmrx-xvogi-adg5n-dowp3-qasvb-nvdjl-ijj3m-e6ijc-pmrmb-dqe,,mohjv-bqaaa-aaaag-qjyia-cai,0000000000000000000000000000000000000000000000000000000000000000,bca85666f3d2de1d135d399a9ccc96d38090601e784e1e5467b87fb3334ec1e0,mohjv-bqaaa-aaaag-qjyia-cai,ICPSwap:ICP/ckUSDC,xevnm-gaaaa-aaaar-qafnq-cai,ckUSDC,ckUSDC,1,ryjl3-tyaaa-aaaaa-aaaba-cai,Internet Computer,ICP,2.3380005528238257,51.914655,0,0,22.20472315,0.01,0.0001,51.914655,51.914655,12092899856764710641614002586.0000000000000000,,-37597,0,9030017452143"

B_CSV = (
    B_HEADER
    + "\r\n"
    + "\r\n".join(B_ROWS + [B_UNIQUE_IN_WINDOW_ROW, B_OUT_OF_WINDOW_ROW])
    + "\r\n"
).encode("utf-8")


def _parse_rows(csv_bytes: bytes) -> list[dict[str, str]]:
    import csv
    import io

    return list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8"))))


def test_overlap_window_produces_exactly_the_expected_duplicate_count():
    """All 10 A-window rows in the sanitized fixtures have exactly one B duplicate
    each, at 0-second tolerance."""
    index = dataset_a_window_index(A_CSV, WINDOW_START, WINDOW_END, max_records=1000)
    assert len(index) == 10, index

    cleaned, report = clean_dataset_b_overlap(
        B_CSV, index, WINDOW_START, WINDOW_END, tolerance_seconds=0, max_records=1000
    )
    assert report["dataset_b_total_rows"] == 12
    assert report["dataset_b_window_rows"] == 11  # 10 dup + 1 unique-in-window
    assert report["dropped_duplicate_rows"] == 10
    assert report["kept_rows"] == 2

    kept = _parse_rows(cleaned)
    assert len(kept) == 2
    kept_hashes = {row["tx_hash"] for row in kept}
    assert kept_hashes == {
        "xmiu5-jqaaa-aaaag-qbz7q-cai488819",  # unique-in-window, must survive
        "mohjv-bqaaa-aaaag-qjyia-cai875344",  # out-of-window, must survive
    }


def test_non_duplicate_rows_are_preserved_byte_for_byte():
    """Rows that are kept must come through with every original column value
    untouched - this is a surgical row-drop, not a reformat."""
    index = dataset_a_window_index(A_CSV, WINDOW_START, WINDOW_END, max_records=1000)
    cleaned, _report = clean_dataset_b_overlap(
        B_CSV, index, WINDOW_START, WINDOW_END, tolerance_seconds=0, max_records=1000
    )
    kept = {row["tx_hash"]: row for row in _parse_rows(cleaned)}
    original = {row["tx_hash"]: row for row in _parse_rows(B_CSV)}
    for tx_hash in ("xmiu5-jqaaa-aaaag-qbz7q-cai488819", "mohjv-bqaaa-aaaag-qjyia-cai875344"):
        assert kept[tx_hash] == original[tx_hash]


def test_no_match_window_leaves_dataset_b_file_untouched():
    """An empty Dataset-A index (e.g. the window genuinely has no Dataset-A
    coverage) must drop nothing - fail-safe toward keeping data, not losing it."""
    cleaned, report = clean_dataset_b_overlap(
        B_CSV, [], WINDOW_START, WINDOW_END, tolerance_seconds=0, max_records=1000
    )
    assert report["dropped_duplicate_rows"] == 0
    assert report["kept_rows"] == report["dataset_b_total_rows"] == 12


def test_each_dataset_a_row_consumed_at_most_once():
    """A single real A row's content key must never drop more than one B row,
    even if (hypothetically) more than one B row shared that exact key."""
    index = dataset_a_window_index(A_CSV, WINDOW_START, WINDOW_END, max_records=1000)
    assert len(index) == 10
    # Duplicate the whole B file's matching rows: two physically-identical rows per
    # genuine A entry. Only one of each pair may be dropped per A entry.
    doubled_b = (
        B_HEADER
        + "\r\n"
        + "\r\n".join(B_ROWS + B_ROWS + [B_UNIQUE_IN_WINDOW_ROW, B_OUT_OF_WINDOW_ROW])
        + "\r\n"
    ).encode("utf-8")
    cleaned, report = clean_dataset_b_overlap(
        doubled_b, index, WINDOW_START, WINDOW_END, tolerance_seconds=0, max_records=1000
    )
    assert report["dropped_duplicate_rows"] == 10
    # 20 doubled rows + 1 unique-in-window + 1 out-of-window = 22 total, minus 10 drops = 12.
    assert report["dataset_b_total_rows"] == 22
    assert report["kept_rows"] == 12
    kept = _parse_rows(cleaned)
    # 10 surviving duplicate-content rows (the un-consumed twin of each pair) + the
    # unique-in-window row + the out-of-window row.
    assert len(kept) == 12


def test_column_count_mismatches_are_kept_malformed_for_downstream_quarantine():
    """M11: a B row with extra cells used to lose them and a short row was padded with empty
    strings, so a malformed row came out of the reconciliation WELL-FORMED and was persisted
    as a fact. Such rows must reach ingestion with their original cell count so the CSV
    boundary quarantines them (``column_count``) with every original value."""
    from silver_csv import normalize_csv

    extra = B_OUT_OF_WINDOW_ROW + ",EXTRA-CELL"
    short = ",".join(B_OUT_OF_WINDOW_ROW.split(",")[:-3])
    b_csv = (
        B_HEADER + "\r\n" + "\r\n".join([extra, short, B_UNIQUE_IN_WINDOW_ROW]) + "\r\n"
    ).encode("utf-8")
    cleaned, report = clean_dataset_b_overlap(
        b_csv, [], WINDOW_START, WINDOW_END, tolerance_seconds=0, max_records=1000
    )
    observed = []
    records, issues = normalize_csv(
        cleaned.decode("utf-8"),
        "B",
        lambda ordinal, _row: {
            "lineage_ref": f"x#{ordinal}",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        },
        observation_sink=lambda o, line, raw, row_issues, rec: observed.append((raw, row_issues)),
    )
    assert len(records) == 1, records
    assert [i["code"] for i in issues].count("column_count") == 2, issues
    quarantined = [raw for raw, row_issues in observed if row_issues and "extra_columns" in raw]
    assert ["EXTRA-CELL"] in [raw["extra_columns"] for raw in quarantined], quarantined
    assert report["dataset_b_total_rows"] == 3, report
    assert report.get("column_count_rows") == 2, report


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    sys.exit(1 if failures else 0)
