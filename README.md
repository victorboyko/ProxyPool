ProxyPool - stratum equihash hashing power profit switcher

Equihash stratum server that redirects hashing power to currenly most profitable coin stratum server (mining pool).

- allows to configure the coins and their pool credentials list encoded in a statum login
- uses MineHub (https://github.com/victorboyko/MineHub) for profitability information
- incoming hashing traffic (shares) statistics page
- current profitability page

- extranonce.subsribe enabled mode for miners that support it
- NiceHash mode (no extranonce.subscribe) for taking hashing traffic from external sources (for ex. - NiceHash):
  Nonce is passed to destination pools instead of a miner software name in a mining.subscribe stratum message. 
  In this case in order for all nonces to be in synch on destination pools - all except 1 destination pools must accept nonce in mining.subscribe.
  This can be achieved for ex. by slight modification of equihash-solomining (https://github.com/aayanl/equihash-solomining) stratum server and running 
  a separate instance per coin. One of the destination pools however can be external, its nonce will be used for the rest of pools.