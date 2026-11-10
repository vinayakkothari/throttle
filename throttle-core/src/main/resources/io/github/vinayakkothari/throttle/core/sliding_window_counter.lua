-- Sliding window counter: two fixed windows blended by how far into the current one we are.
-- Constant memory per key and smooth at window edges, at the cost of being an estimate.
--
-- KEYS[1]  hash holding {idx = current window number, cur = its count, prev = previous count}
-- ARGV[1]  limit per window
-- ARGV[2]  window in ms
-- ARGV[3]  requests wanted
-- ARGV[4]  '1' to grant as many as are available, '0' for all or nothing
--
-- Both windows live in one hash so the script touches a single declared key, which keeps it
-- valid on Redis Cluster.
--
-- Returns {granted, remaining, retry after ms, -1 if never possible}
if redis.replicate_commands then redis.replicate_commands() end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local want = tonumber(ARGV[3])
local partial = ARGV[4] == '1'

local idx = math.floor(now / window)
local state = redis.call('HMGET', KEYS[1], 'idx', 'cur', 'prev')
local sidx = tonumber(state[1])
local cur = tonumber(state[2]) or 0
local prev = tonumber(state[3]) or 0
if sidx == nil then
  cur, prev = 0, 0
elseif sidx == idx - 1 then
  prev, cur = cur, 0
elseif sidx ~= idx then
  cur, prev = 0, 0
end

local into = now - idx * window
local weight = 1 - into / window
local estimate = prev * weight + cur
local free = math.floor(limit - estimate)

local granted = 0
if free >= want then
  granted = want
elseif partial and free > 0 then
  granted = free
end
cur = cur + granted

redis.call('HSET', KEYS[1], 'idx', idx, 'cur', cur, 'prev', prev)
redis.call('PEXPIRE', KEYS[1], window * 2)

local retry = 0
if granted == 0 or (granted < want and not partial) then
  local need = want
  if partial then need = 1 end
  if need > limit then
    retry = -1
  elseif prev > 0 and limit - need - cur >= 0 then
    -- the previous window's weight decays until the estimate leaves room
    local f = 1 - (limit - need - cur) / prev
    retry = math.ceil(f * window) - into
  else
    -- room only appears in the next window, once this window becomes "previous"
    local f = 0
    if cur > 0 then f = 1 - (limit - need) / cur end
    if f < 0 then f = 0 end
    retry = (window - into) + math.ceil(f * window)
  end
  if retry ~= -1 and retry < 1 then retry = 1 end
end

local remaining = math.floor(limit - estimate - granted)
if remaining < 0 then remaining = 0 end
return {granted, remaining, retry}
