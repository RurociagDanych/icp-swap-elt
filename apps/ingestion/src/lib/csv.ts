import type { SwapTx } from '../idl/poolInfo.js';

export type CsvRow = {
  storage_canister: string;
  hash: string;
  timestamp: string;
  action: string;
  pool_id: string;
  sender: string;
  from: string;
  to: string;
  recipient: string;
  token0_id: string;
  token0_symbol: string;
  token0_standard: string;
  token0_change: number;
  token0_price: number;
  token0_decimals: number;
  token1_id: string;
  token1_symbol: string;
  token1_standard: string;
  token1_change: number;
  token1_price: number;
  token1_decimals: number;
  amount_usd: number;
  amount_token0: number;
  amount_token1: number;
  pool_fee: string;
  tick: string;
  liquidity_total: string;
  liquidity_change: string;
};

export const headers: (keyof CsvRow)[] = [
  'storage_canister','hash','timestamp','action','pool_id','sender','from','to','recipient',
  'token0_id','token0_symbol','token0_standard','token0_change','token0_price','token0_decimals',
  'token1_id','token1_symbol','token1_standard','token1_change','token1_price','token1_decimals',
  'amount_usd','amount_token0','amount_token1','pool_fee','tick','liquidity_total','liquidity_change'
];

export function rowFromTx(storageId: string, tx: SwapTx): CsvRow {
  const action = Object.keys(tx.action || {})[0] ?? 'unknown';
  return {
    storage_canister: storageId,
    hash: tx.hash,
    timestamp: tx.timestamp.toString(),
    action,
    pool_id: tx.poolId,
    sender: tx.sender,
    from: tx.from,
    to: tx.to,
    recipient: tx.recipient,
    token0_id: tx.token0Id,
    token0_symbol: tx.token0Symbol,
    token0_standard: tx.token0Standard,
    token0_change: tx.token0ChangeAmount,
    token0_price: tx.token0Price,
    token0_decimals: tx.token0Decimals,
    token1_id: tx.token1Id,
    token1_symbol: tx.token1Symbol,
    token1_standard: tx.token1Standard,
    token1_change: tx.token1ChangeAmount,
    token1_price: tx.token1Price,
    token1_decimals: tx.token1Decimals,
    amount_usd: tx.amountUSD,
    amount_token0: tx.amountToken0,
    amount_token1: tx.amountToken1,
    pool_fee: tx.poolFee.toString(),
    tick: tx.tick.toString(),
    liquidity_total: tx.liquidityTotal.toString(),
    liquidity_change: tx.liquidityChange.toString(),
  };
}
