-- Sliding window log: remember the time of every granted request in a sorted set.
-- Exact, but memory grows with the limit, so it suits small limits like 5 logins a minute.
--
-- KEYS[1]  sorted set, score = time in ms, member = unique id
-- ARGV[1]  limit per window
-- ARGV[2]  window in ms
-- ARGV[3]  requests wanted
-- ARGV[4]  '1' to grant as many as are available, '0' for all or nothing
-- ARGV[5]  random nonce from the caller, keeps members unique across callers
--
-- Returns {granted, remaining, retry after ms, -1 if never possible}
if redis.replicate_commands then redis.replicate_commands() end

local t = redis.call('TIME')
local now_us = tonumber(t[1]) * 1000000 + tonumber(t[2])
local now = math.floor(now_us / 1000)

local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local want = tonumber(ARGV[3])
local partial = ARGV[4] == '1'

-- anything at or before now - window has left the window
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local count = redis.call('ZCARD', KEYS[1])
local free = limit - count

local granted = 0
if free >= want then
  granted = want
elseif partial and free > 0 then
  granted = free
end
for i = 1, granted do
  redis.call('ZADD', KEYS[1], now, now_us .. ':' .. ARGV[5] .. ':' .. i)
end
redis.call('PEXPIRE', KEYS[1], window)

local retry = 0
if granted == 0 or (granted < want and not partial) then
  local need = want
  if partial then need = 1 end
  if need > limit then
    retry = -1
  else
    -- wait until enough of the oldest entries have aged out
    local expire = count - limit + need
    local oldest = redis.call('ZRANGE', KEYS[1], expire - 1, expire - 1, 'WITHSCORES')
    retry = tonumber(oldest[2]) + window - now
    if retry < 1 then retry = 1 end
  end
end

local remaining = limit - count - granted
if remaining < 0 then remaining = 0 end
return {granted, remaining, retry}
