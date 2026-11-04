import { IDL } from '@dfinity/candid';

type IDLType = typeof IDL;

export const baseStorageidlFactory = ({ IDL }: { IDL: IDLType }) => {
  const TransactionType = IDL.Variant({
    decreaseLiquidity: IDL.Null,
    claim: IDL.Null,
    swap: IDL.Null,
    addLiquidity: IDL.Null,
    transferPosition: IDL.Nat,
    increaseLiquidity: IDL.Null,
  });

  const SwapRecordInfo = IDL.Record({
    to: IDL.Text,
    feeAmount: IDL.Int,
    action: TransactionType,
    feeAmountTotal: IDL.Int,
    token0Id: IDL.Text,
    token1Id: IDL.Text,
    token0AmountTotal: IDL.Nat,
    liquidityTotal: IDL.Nat,
    from: IDL.Text,
    tick: IDL.Int,
    feeTire: IDL.Nat,
    recipient: IDL.Text,
    token0ChangeAmount: IDL.Nat,
    token1AmountTotal: IDL.Nat,
    liquidityChange: IDL.Nat,
    token1Standard: IDL.Text,
    token0Fee: IDL.Nat,
    token1Fee: IDL.Nat,
    timestamp: IDL.Int,
    token1ChangeAmount: IDL.Nat,
    token0Standard: IDL.Text,
    price: IDL.Nat,
    poolId: IDL.Text,
  });

  return IDL.Service({
    baseStorage: IDL.Func([], [IDL.Vec(IDL.Text)], ['query']),
    baseLastStorage: IDL.Func([], [IDL.Text], ['query']),
    getDataQueue: IDL.Func([], [IDL.Vec(SwapRecordInfo)], ['query']),
    getStorageCount: IDL.Func([], [IDL.Vec(IDL.Tuple(IDL.Text, IDL.Nat)), IDL.Int], ['query']),
  });
};

export type BaseStorageActor = {
  baseStorage: () => Promise<string[]>;
};
