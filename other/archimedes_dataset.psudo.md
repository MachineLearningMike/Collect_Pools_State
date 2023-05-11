# Combine STGperBlock.csv with STGAllocationPoint.csv to get STGPerPool.csv

```unixtime,block,farming_pools.csv
chainId, index, poolId, startBlock, endBlock, allocPoint
1,0,1, 3895
1,1,2, 3489
1,2,13, 2616
2,0,2, 4897
2,1,5, 5103
```

# Script to merge into a time series data CSV

```py
# Pool Id d is actually combination of chainId and poolId 1-1 1-2 ...

TL(-1, d) := 0
# TLi(-1, d) := 0 # TL, but initiated version
TB(-1, d) := 0
LFC(-1, d) := 0
PFC(-1, d) := 0
EQB(-1, d) := 0
TLP(-1, d) := 0

start_time = earliest([pool_log_start_time(d) for d in pools])
end_time = earliest([pool_log_end_time(d) for d in pools])
# we have confirmed data for range - [start_time, end_time]

for T in timeslots(start_time, end_time): # ascending order T in [0, 1, 2, ...]
  # Initialize values for the timeframe
  for pool as d in pools:
    # d is id of pool
    TL(T, d) := TL(d, T-1)
    # TLi(T, d) := TLi(d, T-1)
    TLD(T, d) := 0
    TLPD(T, d) := 0
    LGR(T, d) :=1
    LW(T, d) := 0
    LA(T, d) := 0
    LFD(T, d) := 0
    PFD(T, d) := 0
    EQF(T, d) := 0
    EQR(T, d) := 0
    TXAs(T, d) := 0
    for other_pool as s in pools and other_pool != pool:
      TXAi(T, d, s) := 0
      # LGRi(T, d, s) := 1

  # Iterate through events
  for dest_pool as d in pools:
    mint_events = get_events(dest_pool, "Mint", gte=timeslot.start, lt=timeslot.end).sort_by(timestamp, "ascending")
    burn_events = get_events(dest_pool, "Burn", gte=timeslot.start, lt=timeslot.end).sort_by(timestamp, "ascending")
    swap_events = get_events(dest_pool, "Swap", gte=timeslot.start, lt=timeslot.end).sort_by(timestamp, "ascending")
    swap_remote_events = get_events(dest_pool, "Swap", gte=timeslot.start, lt=timeslot.end).sort_by(timestamp, "ascending")
    liq_events = (mint_events + burn_events + swap_events).sort_by(timestamp, "ascending") # the events that affect TL(total liquidity)
    for liq_event in liq_events:
      if liq_event[FIELD_EVENTTYPE] == "Mint":
          TL(T, d) += liq_event.amountSD
          TLD(T, d) += liq_event.amountSD
          TLPD(T, d) += liq_event.amountLP
          TBD(T, d) += liq_event.amountSD
          LA(T, d) += liq_event.amountSD
      elif liq_event[FIELD_EVENTTYPE] == "Burn":
          TL(T, d) -= liq_event.amountSD
          TLD(T, d) -= liq_event.amountSD
          TLPD(T, d) -= liq_event.amountLP
          TBD(T, d) -= liq_event.amountSD
          LW(T, d) += liq_event.amountSD
      elif liq_event[FIELD_EVENTTYPE] == "SwapRemote":
          TLD(T, d) += liq_event.dstFee
          newTL(T, d) = TL(T, d) + liq_event.dstFee
          LGR(T,d) *= (newTL(T, d) / TL(T, d))
          TL(T, d) = newTL(T, d)
          TBD(T, d) -= liq_event.amountSD
          LFD(T, d) += liq_event.lpFee
          PFD(T, d) += liq_event.protocolFee
          TXAs(T, d) += liq_event.amountSD
      elif liq_event[FIELD_EVENTTYPE] == "Swap":
          TBD(T, d) += liq_event.amountSD
          EQF(T, poolId(liq_event.dstChain, liq_event.dstPool)) += liq_event.eqFee
          EQR(T, d) += liq_event.eqReward
          TXAi(T, poolId(liq_event.dstChain, liq_event.dstPool), d) += liq_event.amountSD
          # LGRi(T, poolId(liq_event.dstChain, liq_event.dstPool), d)
  
  # Deduce values
  for dest_pool as d in pools:
    TLP(T, d) = TLP(T-1, d) + TLPD(T, d)
    TB(T, d) = TB(T-1, d) + TBD(T, d)
    LFC(T, d) = LFC(T-1, d) + LFD(T, d)
    PFC(T, d) = PFC(T-1, d) + PFD(T, d)
    EQB(T, d) = EQB(T-1, d) + EQF(T, d) - EQR(T, d)

  # Farming related fields
  # NOTE: For now (c,d) and (d) is interchangable. Because d contains chain info.
  STK(c, d, b) := Staked LP token in chain c, poolId d, blocknumber b # plused by deposit, minused by withdraw
  STGpB(c, d, b) := STG per block in chain c, poolId d, blocknumber b
  STGP(t) := STG price at the moment t # from 1-min interval STG price data
  for c in chains:
    for d in pools(c):
      for T in timeslots(start_time, end_time):
        STGM(T, d) = 0
        LPpL(T, d) = TLP(T-1, d) / TL(T-1, d) # LP token minted per L token added to pool.
        STGpLP(T, d) = 0
        PROFpLP(T, d) = 0
        for b in blocks(c, T): # blocks in chain c for time slot T
          STGM(T, d) += STGpB(c, d, b)
          STGpLP(T, d) += STGpB(c, d, b) / STK(c, d, b)
          PROFpLP(T, d) += STGpB(c, d, b) / STK(c, d, b) * STGP(time(c, b))
        FRoI(T, d) = PROFpLP(T, d) * LPpL(T, d)
  # FP(chainId, index) = {poolId:}
  FP(1,0) = {poolId:1} # farming pool number 0 of ETH has S*USDC as lpToken
  # ...


  # Assertions
    

  # Save
  header = [
    "LGR(1-1)",
    "TL(1-1)",
    "TLD(1-1)",
    "TB(1-1)",
    "TBD(1-1)",
    "LW(1-1)",
    "LA(1-1)",
    "LFC(1-1)",
    "LFD(1-1)",
    "PFC(1-1)",
    "PFD(1-1)",
    "EQF(1-1)",
    "EQB(1-1)",
    "EQR(1-1)",
    "TXAs(1-1)",
    "EQF(1-1)<-(2-1)",
    "EQF(1-1)<-(2-2)",
    ...,
    "EQR(1-1)<-(2-1)",
    "EQR(1-1)<-(2-2)",
    ...,
    "TXAi(1-1)<-(2-1)",
    "TXAi(1-1)<-(2-2)",
    ...,
  ]
  record(T) = [
    [
      LGR(dstPoolId),
      TL(dstPoolId),
      TLD(dstPoolId),
      TB(dstPoolId),
      TBD(dstPoolId),
      LW(dstPoolId),
      LA(dstPoolId),
      LFC(dstPoolId),
      LFD(dstPoolId),
      PFC(dstPoolId),
      PFD(dstPoolId),
      EQF(dstPoolId),
      EQB(dstPoolId),
      EQR(dstPoolId),
      TXAs(dstPoolId),
      EQF(dstPoolId, srcPoolId),
      EQF(dstPoolId, srcPoolId),
      ...,
      EQR(dstPoolId, srcPoolId),
      EQR(dstPoolId, srcPoolId),
      ...,
      TXAi(dstPoolId, srcPoolId),
      TXAi(dstPoolId, srcPoolId),
      ...,
    ],
  ]

  save_csv(header, record)
  ```