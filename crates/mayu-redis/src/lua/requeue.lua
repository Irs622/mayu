-- KEYS[1]=stream ; ARGV[1]=group ARGV[2]=entry id ARGV[3]=header ARGV[4]=payload
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('XADD', KEYS[1], '*', 'h', ARGV[3], 'p', ARGV[4])
return 1
