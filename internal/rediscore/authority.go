package rediscore

// primaryScript validates the caller's already observed primary without another
// round trip. Promotion replaces the operation-local store binding; it never
// makes retained SIP records into a resident authority or recalibrates a clock.
const primaryScript = `
local function primary(expected)
 if type(expected)~='string' or #expected~=40 or not string.match(expected,'^[a-f0-9]+$') then return false end
 local info=redis.call('INFO','server')
 local id=string.match(info,'\r\nrun_id:([a-f0-9]+)\r\n')
 local replication=redis.call('INFO','replication')
 return id==expected and string.find(replication,'\r\nrole:master\r\n',1,true)~=nil
end
`

func validPrimary(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			if c < 'a' || c > 'f' {
				return false
			}
		}
	}
	return true
}
