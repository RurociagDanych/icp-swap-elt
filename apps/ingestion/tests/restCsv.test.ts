import assert from 'node:assert/strict';
import { test } from 'node:test';
import { restHeaders, rowFromRestTx } from '../src/lib/restCsv.js';
import type { RestTx } from '../src/idl/restTx.js';

const sample: RestTx = {
  poolId: 'ttnzy-lyaaa-aaaag-qj2bq-cai', poolFee: 3000, positionId: 0,
  token0LedgerId: 'ryjl3-tyaaa-aaaaa-aaaba-cai', token0Price: '2.27', token0Name: 'Internet Computer', token0Symbol: 'ICP',
  token1LedgerId: 'lkwrt-vyaaa-aaaaq-aadhq-cai', token1Price: '0.0008', token1Name: 'ORIGYN', token1Symbol: 'OGY',
  actionType: 'Swap',
  fromPrincipalId: 'tg3k5', fromSubaccount: '00', fromAccountId: '1ef5', fromTextualId: 'tg3k5', fromAlias: null,
  toPrincipalId: 'ttnzy', toSubaccount: '00', toAccountId: 'fd09', toTextualId: 'ttnzy', toAlias: 'ICPSwap:OGY/ICP',
  token0AmountIn: '0.29', token1AmountIn: '0', token0AmountOut: '0', token1AmountOut: '784.96',
  token0Fee: '0.0001', token1Fee: '0.002', sqrtPrice: '15451', tickLimit: '', tick: '-78749',
  liquidity: '0', currentLiquidity: '742335', txHash: 'ttnzy-lyaaa-aaaag-qj2bq-cai244431', txTime: 1782127219000,
  token0TxValue: '0.68', token1TxValue: '0.68',
};

test('rowFromRestTx maps every header column', () => {
  const row = rowFromRestTx(sample);
  for (const h of restHeaders) assert.ok(h in row, `missing column ${h}`);
  assert.equal(row.tx_hash, 'ttnzy-lyaaa-aaaag-qj2bq-cai244431');
  assert.equal(row.tx_time, 1782127219000);
  assert.equal(row.from_alias, ''); // null -> empty string
  assert.equal(row.action_type, 'Swap');
});
