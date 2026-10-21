-- KEYS[1]=workflow hash
-- Stops the workflow from releasing further nodes. Returns 0 if the hash is missing or every node is
-- done (nothing left to cancel), else sets status=cancelled and returns 1.
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
local done, nodes = tonumber(redis.call('HGET', KEYS[1], 'done')), tonumber(redis.call('HGET', KEYS[1], 'nodes'))
if not done or not nodes then
  return redis.error_reply('malformed workflow ' .. KEYS[1] .. ': no done/nodes counts')
end
if done >= nodes then
  return 0
end
redis.call('HSET', KEYS[1], 'status', 'cancelled')
return 1
