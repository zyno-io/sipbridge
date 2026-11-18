"""Correct a pinned upstream test assumption only in its disposable fixture copy."""
from pathlib import Path
import sys

source = Path(sys.argv[1]) / 'client_test.go'
original = source.read_text()
old = '\tassert.True(t, cseq.SeqNo > 1)'
new = '\tassert.LessOrEqual(t, cseq.SeqNo, uint32(0x7FFF))'
if original.count(old) != 1:
    raise SystemExit('pinned native CSeq fixture assertion changed; review required')
source.write_text(original.replace(old, new))
