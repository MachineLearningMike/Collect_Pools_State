
head_list = {'TS_FROM', 'TS_TO', 'STGM', 'FROI'}
pool_id_list = {
    "USDC": 1,
    "USDT": 2,
    "BUSD": 5,
    "SGETH": 13,
}

chain_id_list = {
    "ethereum": 1,
    "bsc": 2,
    "avalanche": 6,
    "polygon": 9,
    "arbitrum": 10,
    "optimism": 11,
    "fantom": 12,
}

event_topics = ['0x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15','0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568']
tbl_schema = [
	{'name': 'pool_id', 'type': 'int64'},
    {'name': 'block_no', 'type': 'int64'},
    {'name': 'timestamp', 'type': 'int64'},
    {'name': 'tx', 'type': 'string'},
    {'name': 'event_type', 'type': 'int64'},
    {'name': 'amount', 'type': 'float64'}
]

pool_topic_list = {
    'ethereum': {
        '0x0000000000000000000000000000000000000000000000000000000000000000': 'USDC',
        '0x0000000000000000000000000000000000000000000000000000000000000001': 'USDT',
        '0x0000000000000000000000000000000000000000000000000000000000000002': 'SGETH',
    },
    'bsc': {
        '0x0000000000000000000000000000000000000000000000000000000000000000': 'USDT',
        '0x0000000000000000000000000000000000000000000000000000000000000001': 'BUSD',
    },
    'avalanche': {
        '0x0000000000000000000000000000000000000000000000000000000000000000': 'USDC',
        '0x0000000000000000000000000000000000000000000000000000000000000001': 'USDT',
    },
    'polygon': {
        '0x0000000000000000000000000000000000000000000000000000000000000000': 'USDC',
        '0x0000000000000000000000000000000000000000000000000000000000000001': 'USDT',
    },
    'arbitrum': {
        '0x0000000000000000000000000000000000000000000000000000000000000000': 'USDC',
        '0x0000000000000000000000000000000000000000000000000000000000000001': 'USDT',
        '0x0000000000000000000000000000000000000000000000000000000000000002': 'SGETH',
    },
    'optimism': {
        '0x0000000000000000000000000000000000000000000000000000000000000000': 'USDC',
        '0x0000000000000000000000000000000000000000000000000000000000000001': 'SGETH',
    },
    'fantom': {
        '0x0000000000000000000000000000000000000000000000000000000000000000': 'USDC',
    },
}

chain_info_list = {
	'ethereum': {
		'address': {
			'USDC': {
				'poolAddress': "0xdf0770dF86a8034b3EFEf0A1Bb3c889B8332FF56",
				'erc20Address': "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
				'deployBlock': 14403393,
			},
			'USDT': {
				'poolAddress': "0x38EA452219524Bb87e18dE1C24D3bB59510BD783",
				'erc20Address': "0xdAC17F958D2ee523a2206206994597C13D831ec7",
				'deployBlock': 14403402,
			},
			'SGETH': {
				'poolAddress': "0x101816545F6bd2b1076434B54383a1E633390A2E",
				'erc20Address': "0x72E2F4830b9E45d52F80aC08CB2bEC0FeF72eD9c",
				'deployBlock': 15035701,
			},
		},
        'LPStaking': '0xB0D502E938ed5f4df2E681fE6E419ff29631d62b',
		'key': '845JF5MISST352XBEQK6XRS6FJE9ZIFWPB',
		'url': 'https://api.etherscan.io/api'
	},
	'bsc': {
		'address': {
			'USDT': {
				'poolAddress': "0x9aA83081AA06AF7208Dcc7A4cB72C94d057D2cda",
				'erc20Address': "0x55d398326f99059fF775485246999027B3197955",
				'deployBlock': 16135132,
			},
			'BUSD': {
				'poolAddress': "0x98a5737749490856b401DB5Dc27F522fC314A4e1",
				'erc20Address': "0xe9e7cea3dedca5984780bafc599bd69add087d56",
				'deployBlock': 16135136
			},
		},
        'LPStaking': '0x3052A0F6ab15b4AE1df39962d5DdEFacA86DaB47',
		'key': 'RRDA5RRPP8US539A3NNGC5IE8DNTW93V4B',
		'url': 'https://api.bscscan.com/api'
	},
	'avalanche': {
		'address': {
			'USDC': {
				'poolAddress': "0x1205f31718499dBf1fCa446663B532Ef87481fe1",
				'deployBlock': 12219159,
				'erc20Address': "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
			},
			'USDT': {
				'poolAddress': "0x29e38769f23701A2e4A8Ef0492e19dA4604Be62c",
				'deployBlock': 12219171,
				'erc20Address': "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
			},
		},
        'LPStaking': '0x8731d54E9D02c286767d56ac03e8037C07e01e98',
		'key': 'DDJSBNQC91M49F2216CCKMWWWX6EJ4U7DE',
		'url': 'https://api.snowtrace.io/api'
	},
	'polygon': {
		'address': {
			'USDC': {
				'poolAddress': "0x1205f31718499dBf1fCa446663B532Ef87481fe1",
				'deployBlock': 26032726,
				'erc20Address': "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
			},
			'USDT': {
				'poolAddress': "0x29e38769f23701A2e4A8Ef0492e19dA4604Be62c",
				'deployBlock': 26032728,
				'erc20Address': "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
			},
		},
        'LPStaking': '0x8731d54E9D02c286767d56ac03e8037C07e01e98',
		'key': 'HBIW4HP9DBQM8VE4W9MRNMYUZ9E3R18WMB',
		'url': 'https://api.polygonscan.com/api'
	},
	'arbitrum': {
		'address': {
			'USDC': {
				'poolAddress': "0x892785f33CdeE22A30AEF750F285E18c18040c3e",
				'deployBlock': 8041115,
				'erc20Address': "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
			},
			'USDT': {
				'poolAddress': "0xB6CfcF89a7B22988bfC96632aC2A9D6daB60d641",
				'deployBlock': 8041122,
				'erc20Address': "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
			},
			'SGETH': {
				'poolAddress': "0x915A55e36A01285A14f05dE6e81ED9cE89772f8e",
				'deployBlock': 16112680, #recently deployed
				'erc20Address': "0x82CbeCF39bEe528B5476FE6d1550af59a9dB6Fc0",
			},
		},
        'LPStaking': '0xeA8DfEE1898a7e0a59f7527F076106d7e44c2176',
		'key': 'QGB3WYFBTR648MMACKWR217U7I1GA9WRXG',
		'url': 'https://api.arbiscan.io/api',
	},
	'optimism': {
		'address': {
			'USDC': {
				'poolAddress': "0xDecC0c09c3B5f6e92EF4184125D5648a66E35298",
				'deployBlock': 4535509, # Mar-17-2022 07:52:39 AM +UTC
				'erc20Address': "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
			},
			'SGETH': {
				'poolAddress': "0xd22363e3762cA7339569F3d33EADe20127D5F98C",
				'deployBlock': 13332734, # Jun-27-2022 06:23:43 PM +UTC
				'erc20Address': "0xb69c8CBCD90A39D8D3d3ccf0a3E968511C3856A0",
			},
		},
        'LPStaking': '0x4DeA9e918c6289a52cd469cAC652727B7b412Cd2',
		'key': '65D69EGDF9SD9PG7SCUGCRDI2F6NKJD4XI',
		'url': 'https://api-optimistic.etherscan.io/api'
	},
	'fantom': {
		'address':{
			'USDC': {
				'poolAddress': "0x12edeA9cd262006cC3C4E77c90d2CD2DD4b1eb97",
				'deployBlock': 33647195 , # Mar-17-2022 07:53:27 AM +UTC
				'erc20Address': "0x04068da6c83afcfa0e13ba15a6696662335d5b75",
			},
		},
        'LPStaking': '0x224D8Fd7aB6AD4c6eb4611Ce56EF35Dec2277F03',
		'key': 'HU8VD7IF1INHGT8BR2XZH5UC1X58AISN1X',
		'url': 'https://api.ftmscan.com/api'
	}
}
