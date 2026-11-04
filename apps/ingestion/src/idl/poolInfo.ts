import { IDL } from '@dfinity/candid';

type IDLType = typeof IDL;

export const poolInfoidlFactory = ({ IDL }: { IDL: IDLType }) => {
  const TransactionType = IDL.Variant({
    decreaseLiquidity: IDL.Null,
    claim: IDL.Null,
    swap: IDL.Null,
    addLiquidity: IDL.Null,
    increaseLiquidity: IDL.Null,
  });

  const Transaction = IDL.Record({
    to: IDL.Text,
    action: TransactionType,
    token0Id: IDL.Text,
    token1Id: IDL.Text,
    liquidityTotal: IDL.Nat,
    from: IDL.Text,
    hash: IDL.Text,
    tick: IDL.Int,
    token1Price: IDL.Float64,
    recipient: IDL.Text,
    token0ChangeAmount: IDL.Float64,
    sender: IDL.Text,
    liquidityChange: IDL.Nat,
    token1Standard: IDL.Text,
    token0Fee: IDL.Float64,
    token1Fee: IDL.Float64,
    timestamp: IDL.Int,
    token1ChangeAmount: IDL.Float64,
    token1Decimals: IDL.Float64,
    token0Standard: IDL.Text,
    amountUSD: IDL.Float64,
    amountToken0: IDL.Float64,
    amountToken1: IDL.Float64,
    poolFee: IDL.Nat,
    token0Symbol: IDL.Text,
    token0Decimals: IDL.Float64,
    token0Price: IDL.Float64,
    token1Symbol: IDL.Text,
    poolId: IDL.Text,
  });

  const RecordPage = IDL.Record({
    content: IDL.Vec(Transaction),
    offset: IDL.Nat,
    limit: IDL.Nat,
    totalElements: IDL.Nat,
  });

  return IDL.Service({
    getBaseRecord: IDL.Func([IDL.Nat, IDL.Nat, IDL.Vec(IDL.Text)], [RecordPage], ['query']),
  });
};

export type TxAction = {
  decreaseLiquidity?: null;
  claim?: null;
  swap?: null;
  addLiquidity?: null;
  increaseLiquidity?: null;
};

export type SwapTx = {
  to: string;
  action: TxAction;
  token0Id: string;
  token1Id: string;
  liquidityTotal: bigint;
  from: string;
  hash: string;
  tick: bigint;
  token1Price: number;
  recipient: string;
  token0ChangeAmount: number;
  sender: string;
  liquidityChange: bigint;
  token1Standard: string;
  token0Fee: number;
  token1Fee: number;
  timestamp: bigint;
  token1ChangeAmount: number;
  token1Decimals: number;
  token0Standard: string;
  amountUSD: number;
  amountToken0: number;
  amountToken1: number;
  poolFee: bigint;
  token0Symbol: string;
  token0Decimals: number;
  token0Price: number;
  token1Symbol: string;
  poolId: string;
};

export type RecordPage = {
  content: SwapTx[];
  offset: bigint;
  limit: bigint;
  totalElements: bigint;
};

export type PoolInfoActor = {
  getBaseRecord: (offset: bigint, limit: bigint, filters: string[]) => Promise<RecordPage>;
};
