import type { CsvRecord } from './csvSink.js';
import type { RestTx } from '../idl/restTx.js';

export const restHeaders = [
  'tx_hash','tx_time','action_type','pool_id','pool_fee','position_id',
  'from_principal_id','from_subaccount','from_account_id','from_textual_id','from_alias',
  'to_principal_id','to_subaccount','to_account_id','to_textual_id','to_alias',
  'token0_ledger_id','token0_name','token0_symbol','token0_price',
  'token1_ledger_id','token1_name','token1_symbol','token1_price',
  'token0_amount_in','token1_amount_in','token0_amount_out','token1_amount_out',
  'token0_fee','token1_fee','token0_tx_value','token1_tx_value',
  'sqrt_price','tick_limit','tick','liquidity','current_liquidity',
] as const;

const s = (v: string | null): string => v ?? '';

export function rowFromRestTx(tx: RestTx): CsvRecord {
  return {
    tx_hash: tx.txHash, tx_time: tx.txTime, action_type: tx.actionType,
    pool_id: tx.poolId, pool_fee: tx.poolFee, position_id: tx.positionId,
    from_principal_id: tx.fromPrincipalId, from_subaccount: tx.fromSubaccount,
    from_account_id: tx.fromAccountId, from_textual_id: tx.fromTextualId, from_alias: s(tx.fromAlias),
    to_principal_id: tx.toPrincipalId, to_subaccount: tx.toSubaccount,
    to_account_id: tx.toAccountId, to_textual_id: tx.toTextualId, to_alias: s(tx.toAlias),
    token0_ledger_id: tx.token0LedgerId, token0_name: tx.token0Name, token0_symbol: tx.token0Symbol, token0_price: tx.token0Price,
    token1_ledger_id: tx.token1LedgerId, token1_name: tx.token1Name, token1_symbol: tx.token1Symbol, token1_price: tx.token1Price,
    token0_amount_in: tx.token0AmountIn, token1_amount_in: tx.token1AmountIn,
    token0_amount_out: tx.token0AmountOut, token1_amount_out: tx.token1AmountOut,
    token0_fee: tx.token0Fee, token1_fee: tx.token1Fee, token0_tx_value: tx.token0TxValue, token1_tx_value: tx.token1TxValue,
    sqrt_price: tx.sqrtPrice, tick_limit: tx.tickLimit, tick: tx.tick,
    liquidity: tx.liquidity, current_liquidity: tx.currentLiquidity,
  };
}
