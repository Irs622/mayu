-- KEYS[1]=stream ; ARGV[1]=group ARGV[2]=consumer ARGV[3..]=entry ids
-- Re-claims (JUSTID, no delivery-count bump) only entries this consumer still owns.
-- Returns the ids it no longer owns.
local lost = {}
for i = 3, #ARGV do
  local id = ARGV[i]
  local p = redis.call('XPENDING', KEYS[1], ARGV[1], id, id, 1, ARGV[2])
  if #p > 0 then
    redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, id, 'JUSTID')
  else
    table.insert(lost, id)
  end
end
return lost
