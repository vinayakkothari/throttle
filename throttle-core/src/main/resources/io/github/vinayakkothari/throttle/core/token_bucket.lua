-- Token bucket, refilled lazily from the time of the last call.
--
-- KEYS[1]  hash holding {t = tokens left, ts = last refill time in ms}
-- ARGV[1]  capacity (burst size)
-- ARGV[2]  refill rate in tokens per millisecond
-- ARGV[3]  tokens wanted
-- ARGV[4]  '1' to grant as many as are available (a lease), '0' for all or nothing
--
-- Returns {granted, tokens left (floored), retry after ms, -1 if never possible}
--
-- Time comes from Redis, not from the caller, so app servers with skewed clocks still share
-- one consistent view of time. Redis 7 replicates a script by its effects, which is what
-- makes reading TIME before writing allowed.
if redis.replicate_commands then redis.replicate_commands() end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local want = tonumber(ARGV[3])
local partial = ARGV[4] == '1'

local state = redis.call('HMGET', KEYS[1], 't', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
if now > ts then
  tokens = math.min(capacity, tokens + (now - ts) * rate)
  ts = now
end
-- the limit may have been lowered since the bucket was filled
if tokens > capacity then tokens = capacity end

local granted = 0
if tokens >= want then
  granted = want
elseif partial and tokens >= 1 then
  granted = math.floor(tokens)
end
tokens = tokens - granted

redis.call('HSET', KEYS[1], 't', tostring(tokens), 'ts', tostring(ts))
-- an untouched bucket is full again after capacity / rate ms, so it can simply disappear
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate) + 1000)

local retry = 0
if granted == 0 or (granted < want and not partial) then
  local need = want
  if partial then need = 1 end
  if need > capacity then
    retry = -1
  else
    retry = math.ceil((need - tokens) / rate)
  end
end

return {granted, math.floor(tokens), retry}
