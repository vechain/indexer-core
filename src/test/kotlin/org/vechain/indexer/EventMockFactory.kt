package org.vechain.indexer

import java.math.BigInteger
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.abi.InputOutput
import org.vechain.indexer.event.model.business.*
import org.vechain.indexer.event.model.enums.Operator
import org.vechain.indexer.event.model.generic.AbiEventParameters
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.event.model.generic.RawEvent
import org.vechain.indexer.thor.model.*

object EventMockFactory {
    val transferAbiElement: AbiElement =
        AbiElement(
            name = "Transfer",
            type = "event",
            anonymous = false,
            stateMutability = null,
            inputs =
                listOf(
                    InputOutput("address", "from", "address", indexed = true),
                    InputOutput("address", "to", "address", indexed = true),
                    InputOutput("uint256", "value", "uint256", indexed = false),
                ),
            outputs = emptyList(),
            signature = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        )

    val transferERC721AbiElement: AbiElement =
        AbiElement(
            name = "Transfer",
            type = "event",
            anonymous = false,
            stateMutability = null,
            inputs =
                listOf(
                    InputOutput("address", "from", "address", indexed = true),
                    InputOutput("address", "to", "address", indexed = true),
                    InputOutput("uint256", "value", "uint256", indexed = true),
                ),
            outputs = emptyList(),
            signature = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        )

    val tupleAbiElement: AbiElement =
        AbiElement(
            name = "UpdateMarketItem",
            type = "event",
            anonymous = false,
            stateMutability = null,
            inputs =
                listOf(
                    InputOutput(
                        internalType = "uint256",
                        name = "itemId",
                        type = "uint256",
                        indexed = true,
                        components = null,
                    ),
                    InputOutput(
                        internalType = "uint256",
                        name = "tokenId",
                        type = "uint256",
                        indexed = true,
                        components = null,
                    ),
                    InputOutput(
                        internalType = "address",
                        name = "lister",
                        type = "address",
                        indexed = true,
                        components = null,
                    ),
                    InputOutput(
                        internalType = "struct IMarketplace.MarketItem",
                        name = "updatedItem",
                        type = "tuple",
                        indexed = false,
                        components =
                            listOf(
                                InputOutput(
                                    internalType = "address",
                                    name = "tokenOwner",
                                    type = "address",
                                    indexed = false,
                                    components = null,
                                ),
                                InputOutput(
                                    internalType = "uint256",
                                    name = "itemId",
                                    type = "uint256",
                                    indexed = false,
                                    components = null,
                                ),
                                InputOutput(
                                    internalType = "uint256",
                                    name = "tokenId",
                                    type = "uint256",
                                    indexed = false,
                                    components = null,
                                ),
                                InputOutput(
                                    internalType = "uint256",
                                    name = "startTime",
                                    type = "uint256",
                                    indexed = false,
                                    components = null,
                                ),
                                InputOutput(
                                    internalType = "uint256",
                                    name = "endTime",
                                    type = "uint256",
                                    indexed = false,
                                    components = null,
                                ),
                                InputOutput(
                                    internalType = "uint256",
                                    name = "reserveTokenPrice",
                                    type = "uint256",
                                    indexed = false,
                                    components = null,
                                ),
                                InputOutput(
                                    internalType = "uint256",
                                    name = "buyoutTokenPrice",
                                    type = "uint256",
                                    indexed = false,
                                    components = null,
                                ),
                                InputOutput(
                                    internalType = "enum IMarketplace.ListingType",
                                    name = "listingType",
                                    type = "uint8",
                                    indexed = false,
                                    components = null,
                                ),
                            ),
                    ),
                ),
            outputs = emptyList(),
            signature = "9f1dcc07f753231c0dbaa6d98633dc57553fcae8695bf4d01769eb0bde9c3e19",
        )

    val stringAbiElement: AbiElement =
        AbiElement(
            name = "RewardDistributed",
            type = "event",
            anonymous = false,
            stateMutability = null,
            inputs =
                listOf(
                    InputOutput("uint256", "amount", "uint256", indexed = false),
                    InputOutput("bytes32", "appId", "bytes32", indexed = true),
                    InputOutput("address", "receiver", "address", indexed = true),
                    InputOutput("string", "proof", "string", indexed = false),
                    InputOutput("address", "distributor", "address", indexed = true),
                ),
            outputs = emptyList(),
            signature = "4811710b0c25cc7e05baf214b3a939cf893f1cbff4d0b219e680f069a4f204a2",
        )

    val intAbiElement: AbiElement =
        AbiElement(
            name = "Conversion",
            type = "event",
            anonymous = false,
            stateMutability = null,
            inputs =
                listOf(
                    InputOutput("int8", "tradeType", "int8", indexed = true),
                    InputOutput("address", "_trader", "address", indexed = true),
                    InputOutput("uint256", "_sellAmount", "uint256", indexed = false),
                    InputOutput("uint256", "_return", "uint256", indexed = false),
                    InputOutput("uint256", "_conversionFee", "uint256", indexed = false),
                ),
            outputs = emptyList(),
            signature = "fba23a36f0fad77947f553b9a89c1848ac869c8ce4c1c0d93cb14a9f4ba107f4",
        )

    val arrayAbiElement: AbiElement =
        AbiElement(
            name = "AllocationVoteCast",
            type = "event",
            anonymous = false,
            stateMutability = null,
            inputs =
                listOf(
                    InputOutput(
                        internalType = "address",
                        name = "voter",
                        type = "address",
                        indexed = true,
                        components = null,
                    ),
                    InputOutput(
                        internalType = "uint256",
                        name = "roundId",
                        type = "uint256",
                        indexed = true,
                        components = null,
                    ),
                    InputOutput(
                        internalType = "bytes32[]",
                        name = "appsIds",
                        type = "bytes32[]",
                        indexed = false,
                        components = null,
                    ),
                    InputOutput(
                        internalType = "uint256[]",
                        name = "voteWeights",
                        type = "uint256[]",
                        indexed = false,
                        components = null,
                    ),
                ),
            outputs = listOf(),
            signature = "e2d0d542af9cdd3e0ef4ace292fc5e9dd654164e63920ea9b58c435492af84e2",
        )

    fun createMockTransaction(txOutputs: List<TxOutputs>): Transaction =
        Transaction(
            id = "0xtxID",
            type = 81,
            chainTag = 74,
            blockRef = "0xblockRef",
            expiration = 30,
            clauses = emptyList(),
            gasPriceCoef = null,
            gas = 50000,
            maxFeePerGas = "0xDE0B6B3A7640000",
            maxPriorityFeePerGas = "0xF4240",
            origin = "0xorigin",
            delegator = null,
            nonce = "0x1",
            dependsOn = null,
            size = 100,
            gasUsed = 45000,
            gasPayer = "0xgasPayer",
            paid = "0x100",
            reward = "0x10",
            reverted = false,
            outputs = txOutputs,
        )

    fun createMockBlockWithTransactions(txs: List<Transaction>): Block =
        Block(
            number = 20554260,
            id = "0xblockID",
            size = 6905,
            parentID = "0xparentBlockID",
            timestamp = 1736071230,
            gasLimit = 39882852,
            beneficiary = "0xbeneficiary",
            gasUsed = 1660333,
            totalScore = 2006531030,
            txsRoot = "0xroot",
            txsFeatures = 1,
            stateRoot = "0xstateRoot",
            receiptsRoot = "0xreceiptsRoot",
            com = true,
            signer = "0xsigner",
            isTrunk = true,
            isFinalized = true,
            transactions = txs,
        )

    fun createMockEmptyBlock(): Block =
        Block(
            number = 20554260,
            id = "0xblockID",
            size = 6905,
            parentID = "0xparentBlockID",
            timestamp = 1736071230,
            gasLimit = 39882852,
            beneficiary = "0xbeneficiary",
            gasUsed = 1660333,
            totalScore = 2006531030,
            txsRoot = "0xroot",
            txsFeatures = 1,
            stateRoot = "0xstateRoot",
            receiptsRoot = "0xreceiptsRoot",
            com = true,
            signer = "0xsigner",
            isTrunk = true,
            isFinalized = true,
            transactions = emptyList(),
        )

    val transferEvent =
        TxEvent(
            address = "0x76ca782b59c74d088c7d2cce2f211bc00836c602",
            topics =
                listOf(
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "0x0000000000000000000000008d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                ),
            data = "0x000000000000000000000000000000000000000000000002b5e3af16b1880000",
        )

    val transferEventClause =
        TxOutputs(
            contractAddress = null,
            events =
                listOf(
                    transferEvent,
                ),
            transfers = emptyList(),
        )

    val transferERC71EventClause =
        TxOutputs(
            contractAddress = null,
            events =
                listOf(
                    TxEvent(
                        address = "0x76Ca782B59C74d088C7D2Cce2f211BC00836c602",
                        topics =
                            listOf(
                                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                                "0x0000000000000000000000009c6c2435175dff3974fedce0d4c72c8db3bf5b74",
                                "0x000000000000000000000000c84c3f64f7eefafb62dc53f829219b7393464c45",
                            ),
                        data = "0x00000000000000000000000000000000000000000000000051afa0c39c6d4000",
                    ),
                ),
            transfers = emptyList(),
        )

    val stringEventClause =
        TxOutputs(
            contractAddress = null,
            events =
                listOf(
                    TxEvent(
                        address = "0x76Ca782B59C74d088C7D2Cce2f211BC00836c602",
                        topics =
                            listOf(
                                "0x4811710b0c25cc7e05baf214b3a939cf893f1cbff4d0b219e680f069a4f204a2",
                                "0x2fc30c2ad41a2994061efaf218f1d52dc92bc4a31a0f02a4916490076a7a393a",
                                "0x0000000000000000000000007487c2a053f3bfe4626179b020ccff86ab2b1f5d",
                                "0x000000000000000000000000b6f43457600b1f3b7b98fc4394a9f1134ffc721d",
                            ),
                        data =
                            "0x00000000000000000000000000000000000000000000000008b9bdfc73f9e418000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000001507b2276657273696f6e223a20322c226465736372697074696f6e223a20225468697320697320612070686f746f206f662061207265757361626c65206375702c20726561642061626f757420746865207374756479206f6e20696d706163747320686572653a2068747470733a2f2f6b6565706375702d73747564792e73332e65752d6e6f7274682d312e616d617a6f6e6177732e636f6d2f4b6565704375702b4c43412b5265706f72742e706466222c2270726f6f66223a207b22696d616765223a2268747470733a2f2f626c75727265642d6d756773686f74732e73332e65752d6e6f7274682d312e616d617a6f6e6177732e636f6d2f313733373434303136303131365f696d6167652e706e67227d2c22696d70616374223a207b22636172626f6e223a33372c22656e65726779223a3236332c2274696d626572223a32332c22706c6173746963223a337d7d00000000000000000000000000000000",
                    ),
                ),
            transfers =
                listOf(
                    TxTransfer(
                        sender = "0x54a0ed27c58e4dde7f6c8bbf1f156e1d73a8dc59",
                        recipient = "0x76Ca782B59C74d088C7D2Cce2f211BC00836c602",
                        amount = "50000000000000000000",
                    ),
                    TxTransfer(
                        sender = "0x54a0ed27c58e4dde7f6c8bbf1f156e1d73a8dc59",
                        recipient = "0xb2f12edde215e39186cc7653aeb551c8cf1f77e3",
                        amount = "10000000000",
                    ),
                ),
        )

    val intEventClause =
        TxOutputs(
            contractAddress = null,
            events =
                listOf(
                    TxEvent(
                        address = "0x76Ca782B59C74d088C7D2Cce2f211BC00836c602",
                        topics =
                            listOf(
                                "0xfba23a36f0fad77947f553b9a89c1848ac869c8ce4c1c0d93cb14a9f4ba107f4",
                                "0x0000000000000000000000000000000000000000000000000000000000000000",
                                "0x000000000000000000000000fc5a8bbff0cfc616472772167024e7cd977f27f6",
                            ),
                        data =
                            "0x00000000000000000000000000000000000000000000010f0cf064dd592000000000000000000000000000000000000000000000000010b4976152b645d794c900000000000000000000000000000000000000000000000223ad7287d6cc5eb7",
                    ),
                ),
            transfers = emptyList(),
        )

    val arrayEventClause =
        TxOutputs(
            contractAddress = null,
            events =
                listOf(
                    TxEvent(
                        address = "0x89A00Bb0947a30FF95BEeF77a66AEdE3842Fe5B7",
                        topics =
                            listOf(
                                "0xe2d0d542af9cdd3e0ef4ace292fc5e9dd654164e63920ea9b58c435492af84e2",
                                "0x00000000000000000000000054a0ed27c58e4dde7f6c8bbf1f156e1d73a8dc59",
                                "0x000000000000000000000000000000000000000000000000000000000000001b",
                            ),
                        data =
                            @Suppress("ktlint:standard:max-line-length")
                            "0x000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000029643ed1637948cc571b23f836ade2bdb104de88e627fa6e8e3ffef1ee5a1739a899de0d0f0b39e484c8835b2369194c4c102b230c813862db383d44a4efe14d30000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000021e3b2480264f12000000000000000000000000000000000000000000000000021e3b2480264f12000",
                    ),
                ),
            transfers =
                listOf(
                    TxTransfer(
                        sender = "0x54a0ed27c58e4dde7f6c8bbf1f156e1d73a8dc59",
                        recipient = "0x89A00Bb0947a30FF95BEeF77a66AEdE3842Fe5B7",
                        amount = "50000000000000000000",
                    ),
                ),
        )

    val tupleEventClause =
        TxOutputs(
            contractAddress = null,
            events =
                listOf(
                    TxEvent(
                        address = "0xb2f12edde215e39186cc7653aeb551c8cf1f77e3",
                        topics =
                            listOf(
                                "0x9f1dcc07f753231c0dbaa6d98633dc57553fcae8695bf4d01769eb0bde9c3e19",
                                "0x000000000000000000000000000000000000000000000000000000000000005c",
                                "0x000000000000000000000000000000000000000000000000000000000000234a",
                                "0x00000000000000000000000071b8e51af9280e8bef933586b7de30eb01c0cd92",
                            ),
                        data =
                            @Suppress("ktlint:standard:max-line-length")
                            "0x00000000000000000000000071b8e51af9280e8bef933586b7de30eb01c0cd92000000000000000000000000000000000000000000000000000000000000005c000000000000000000000000000000000000000000000000000000000000234a000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000dc3a8351f3d86a000000000000000000000000000000000000000000000000000000000000000000000",
                    ),
                ),
            transfers = emptyList(),
        )

    val vot3SwapEventDefinition =
        BusinessEventDefinition(
            name = "B3trVot3Swap",
            sameClause = true,
            events =
                listOf(
                    Event(
                        name = "Transfer",
                        alias = "transferB3TR",
                        conditions =
                            listOf(
                                Condition(
                                    firstOperand = "to",
                                    isFirstStatic = false,
                                    secondOperand = "0x76ca782b59c74d088c7d2cce2f211bc00836c602",
                                    isSecondStatic = true,
                                    operator = Operator.EQ,
                                    isNumber = false,
                                ),
                                Condition(
                                    firstOperand = "address",
                                    isFirstStatic = false,
                                    secondOperand = "0x5ef79995fe8a89e0812330e4378eb2660cede699",
                                    isSecondStatic = true,
                                    operator = Operator.EQ,
                                    isNumber = false,
                                ),
                            ),
                    ),
                    Event(
                        name = "Transfer",
                        alias = "transferVOT3",
                        conditions =
                            listOf(
                                Condition(
                                    firstOperand = "from",
                                    isFirstStatic = false,
                                    secondOperand = "0x0000000000000000000000000000000000000000",
                                    isSecondStatic = true,
                                    operator = Operator.EQ,
                                    isNumber = false,
                                ),
                                Condition(
                                    firstOperand = "address",
                                    isFirstStatic = false,
                                    secondOperand = "0x76ca782b59c74d088c7d2cce2f211bc00836c602",
                                    isSecondStatic = true,
                                    operator = Operator.EQ,
                                    isNumber = false,
                                ),
                            ),
                    ),
                ),
            rules =
                listOf(
                    Rule(
                        firstEventName = "transferB3TR",
                        firstEventProperty = "value",
                        secondEventName = "transferVOT3",
                        secondEventProperty = "value",
                        operator = Operator.EQ,
                        isNumber = false,
                    ),
                ),
            paramsDefinition =
                listOf(
                    ParamDefinition(
                        name = "from",
                        eventName = "transferB3TR",
                        businessEventName = "user",
                    ),
                    ParamDefinition(
                        name = "value",
                        eventName = "transferB3TR",
                        businessEventName = "amountB3TR",
                    ),
                    ParamDefinition(
                        name = "value",
                        eventName = "transferB3TR",
                        businessEventName = "amountVOT3",
                    ),
                ),
        )

    val sale =
        BusinessEventDefinition(
            name = "FixedPriceSale",
            sameClause = true,
            events =
                listOf(
                    Event(
                        name = "PurchaseNonCustodial",
                        alias = "sale1",
                        conditions = emptyList(),
                    ),
                    Event(
                        name = "Transfer",
                        alias = "transfer1",
                        conditions = emptyList(),
                    ),
                ),
            rules =
                listOf(
                    Rule(
                        firstEventName = "sale1",
                        firstEventProperty = "tokenId",
                        secondEventName = "transfer1",
                        secondEventProperty = "tokenId",
                        operator = Operator.EQ,
                        isNumber = false,
                    ),
                    Rule(
                        firstEventName = "sale1",
                        firstEventProperty = "buyer",
                        secondEventName = "transfer1",
                        secondEventProperty = "to",
                        operator = Operator.EQ,
                        isNumber = false,
                    ),
                    Rule(
                        firstEventName = "sale1",
                        firstEventProperty = "nft",
                        secondEventName = "transfer1",
                        secondEventProperty = "address",
                        operator = Operator.EQ,
                        isNumber = false,
                    ),
                ),
            paramsDefinition =
                listOf(
                    ParamDefinition(
                        name = "tokenId",
                        eventName = "sale1",
                        businessEventName = "tokenId",
                    ),
                    ParamDefinition(
                        name = "buyer",
                        eventName = "sale1",
                        businessEventName = "buyer",
                    ),
                    ParamDefinition(
                        name = "from",
                        eventName = "transfer1",
                        businessEventName = "seller",
                    ),
                    ParamDefinition(
                        name = "nft",
                        eventName = "sale1",
                        businessEventName = "tokenAddress",
                    ),
                    ParamDefinition(
                        name = "price",
                        eventName = "sale1",
                        businessEventName = "price",
                    ),
                ),
        )

    val b3trSwapVot3Event =
        AbiEventParameters(
            eventType = "Transfer",
            returnValues =
                mapOf(
                    "from" to "0x0000000000000000000000000000000000000000",
                    "to" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "value" to BigInteger("50000000000000000000"),
                ),
        )

    val b3trSwapB3trEvent =
        AbiEventParameters(
            eventType = "Transfer",
            returnValues =
                mapOf(
                    "from" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "to" to "0x76ca782b59c74d088c7d2cce2f211bc00836c602",
                    "value" to BigInteger("50000000000000000000"),
                ),
        )

    val nonB3trSwapVot3Event =
        AbiEventParameters(
            eventType = "Transfer",
            returnValues =
                mapOf(
                    "from" to "0x0000000000000000000000000000000000000000",
                    "to" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "value" to BigInteger("400000000000"),
                ),
        )

    val nonB3trSwapB3trEvent =
        AbiEventParameters(
            eventType = "Transfer",
            returnValues =
                mapOf(
                    "from" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "to" to "0x76ca782b59c74d088c7d2cce2f211bc00836c602",
                    "value" to BigInteger("1000000"),
                ),
        )

    val purchaseEvent =
        AbiEventParameters(
            eventType = "PurchaseNonCustodial",
            returnValues =
                mapOf(
                    "saleId" to 4000000647,
                    "nft" to "0xb603a874d4eaa1d625243f0a416506d62f38a789",
                    "tokenId" to 45,
                    "buyer" to "0x035daf5d3ab419d60d753faa5cb3b8876a97846d",
                    "price" to BigInteger("80000000000000000000"),
                    "code" to "0x7665740000000000000000000000000000000000000000000000000000000000",
                    "startingTime" to 1737498192,
                    "isVIP180" to false,
                    "addressVIP180" to "0x0000000000000000000000000000000000000000",
                ),
        )

    val nftTransferEvent =
        AbiEventParameters(
            returnValues =
                mapOf(
                    "from" to "0xa52b171d88be72f2550f2ffcd166b4825656a9d7",
                    "to" to "0x035daf5d3ab419d60d753faa5cb3b8876a97846d",
                    "tokenId" to 45,
                ),
            eventType = "Transfer",
        )

    fun createIndexedEvent(
        address: String,
        clauseIndex: Long,
        params: AbiEventParameters,
        id: String = "0xid",
    ) =
        IndexedEvent(
            id = id,
            blockId = "0xblockId",
            blockNumber = 20554260,
            blockTimestamp = 1736071230,
            txId = "0xtxId",
            origin = "0xorigin",
            gasPayer = "0xgasPayer",
            address = address,
            raw =
                RawEvent(
                    data = "0xdata",
                    topics = listOf("0xtopic1", "0xtopic2"),
                ),
            clauseIndex = clauseIndex,
            eventType = "Transfer",
            params = params,
            signature = "0xsignature",
        )
}
