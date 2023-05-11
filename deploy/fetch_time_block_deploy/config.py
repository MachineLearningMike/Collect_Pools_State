
chain_id_list = {
    "ethereum": 1,
    "bsc": 2,
    "avalanche": 6,
    "polygon": 9,
    "arbitrum": 10,
    "optimism": 11,
    "fantom": 12,
}

tbl_schema = [
        {'name': 'timestamp', 'type': 'int64'},
        {'name': 'block_no', 'type': 'int64'},
    ]

chain_info_list = {
	'ethereum': {
		'key': 'THS7WRJ7JA4BTJ9J6EKHSBE6K3NXG6CIHM',
		'url': 'https://api.etherscan.io/api'
	},
	'bsc': {
		'key': 'P2MUM15FZ3DJVWJ9CSM5I7K2G1RX8CSQ5Q',
		'url': 'https://api.bscscan.com/api'
	},
	'avalanche': {
		'key': 'ZQNQBSWD5DSS7V4GD9PPZDUG9K4HAAQYGD',
		'url': 'https://api.snowtrace.io/api'
	},
	'polygon': {
		'key': 'AA7G46VWU8ZSBUEAZKMW97HMEZNWJKVZCD',
		'url': 'https://api.polygonscan.com/api'
	},
	'arbitrum': {
		'key': 'MXP9UIYBGVZNQNK11A4J6B5YU2KR3UYSIZ',
		'url': 'https://api.arbiscan.io/api',
	},
	'optimism': {
		'key': 'AX3CBZDRKTF4DKFBVJ36P471Q6MVF2RWKS',
		'url': 'https://api-optimistic.etherscan.io/api'
	},
	'fantom': {
		'key': 'IVMGQMPD9KCTMZC9QHKVWCBE39FVMMDIN4',
		'url': 'https://api.ftmscan.com/api'
	}
}