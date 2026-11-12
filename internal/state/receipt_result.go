package state

// Result policy is independent of marker identity, positive commit evidence,
// quota, and expiry. A discarded refusal is still a committed operation.
func validateReceiptResult(receipt Receipt) error {
	if receipt.ResultUntilMS < 0 || receipt.ResultUntilMS > 1<<53-1 ||
		receipt.ResultUntilMS > 0 && (receipt.ExpiresMS < receipt.ResultUntilMS || receipt.ExpiresMS > 1<<53-1) ||
		receipt.OutcomeDiscarded && (receipt.ResultUntilMS == 0 || len(receipt.Outcome) != 0) {
		return ErrCorrupt
	}
	return nil
}

// receiptResultAt copies the header; it never changes shared outcome bytes or
// renews the original cutoff. Callers use the clock from the same store read.
func receiptResultAt(receipt Receipt, now int64) Receipt {
	if receipt.OutcomeDiscarded || receipt.ResultUntilMS > 0 && now >= receipt.ResultUntilMS {
		receipt.Outcome = nil
		receipt.OutcomeDiscarded = true
	}
	return receipt
}

func projectReceiptResults(record *Record, now int64) {
	for index := range record.Receipts {
		record.Receipts[index] = receiptResultAt(record.Receipts[index], now)
	}
	if record.Proof != nil {
		*record.Proof = receiptResultAt(*record.Proof, now)
	}
}

// Lua works only on decoded local tables until the final successful group
// commit. Read projection and failed CAS never compact the stored envelope.
const receiptResultLua = `
local function receiptResultValid(r)
 local cutoff=tonumber(r.resultUntilMs or '0')
 if not cutoff or cutoff<0 or cutoff>9007199254740991 or cutoff~=math.floor(cutoff) then return false end
 if cutoff>0 then
  local expires=tonumber(r.expiresMs)
  if not expires or expires<cutoff or expires>9007199254740991 then return false end
 end
 if r.outcomeDiscarded~=nil and type(r.outcomeDiscarded)~='boolean' then return false end
 if r.outcomeDiscarded and (cutoff==0 or r.outcome~=nil and r.outcome~='') then return false end
 return true
end
local function receiptResultAt(r,now)
 if r.outcomeDiscarded or tonumber(r.resultUntilMs or '0')>0 and tonumber(r.resultUntilMs)<=now then
  r.outcome=nil
  r.outcomeDiscarded=true
 end
 return r
end
`
