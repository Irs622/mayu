-- KEYS = streams, highest priority first ; ARGV[1]=group ARGV[2]=consumer ARGV[3]=max total
-- Claims up to ARGV[3] new entries, draining earlier streams first.
-- A stream whose group is missing (NOGROUP) is skipped and reported, so entries already
-- claimed from earlier streams are still returned (scripts do not roll back).
-- Returns {claimed, missing} where
--   claimed = {{key_index, id, {field, value, ...}}, ...}
--   missing = {key_index, ...}   (streams without the group)
local left = tonumber(ARGV[3])
local out = {}
local missing = {}
for i, key in ipairs(KEYS) do
  if left <= 0 then break end
  local ok, r = pcall(redis.call, 'XREADGROUP', 'GROUP', ARGV[1], ARGV[2], 'COUNT', left, 'STREAMS', key, '>')
  if not ok then
    local msg = r
    if type(r) == 'table' then msg = r.err end
    if string.find(tostring(msg), 'NOGROUP', 1, true) then
      table.insert(missing, i)
    else
      error(r)
    end
  elseif r then
    for _, e in ipairs(r[1][2]) do
      table.insert(out, {i, e[1], e[2]})
      left = left - 1
    end
  end
end
return {out, missing}
