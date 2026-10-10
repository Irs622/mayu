-- KEYS[1]=delayed KEYS[2]=stream ; ARGV[1]=now ms ARGV[2]=limit
-- Member format: "<header_len>:" .. header .. payload
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, m in ipairs(due) do
  local colon = string.find(m, ':', 1, true)
  local hlen = tonumber(string.sub(m, 1, colon - 1))
  local h = string.sub(m, colon + 1, colon + hlen)
  local p = string.sub(m, colon + hlen + 1)
  redis.call('XADD', KEYS[2], '*', 'h', h, 'p', p)
  redis.call('ZREM', KEYS[1], m)
end
return #due
