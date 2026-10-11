-- KEYS = streams, highest priority first ; ARGV[1]=group ARGV[2]=consumer ARGV[3]=max total
-- Claims up to ARGV[3] new entries, draining earlier streams first.
-- Returns {{key_index, id, {field, value, ...}}, ...}
local left = tonumber(ARGV[3])
local out = {}
for i, key in ipairs(KEYS) do
  if left <= 0 then break end
  local r = redis.call('XREADGROUP', 'GROUP', ARGV[1], ARGV[2], 'COUNT', left, 'STREAMS', key, '>')
  if r then
    for _, e in ipairs(r[1][2]) do
      table.insert(out, {i, e[1], e[2]})
      left = left - 1
    end
  end
end
return out
