const { PubSub } = require("@google-cloud/pubsub")
const { BigQuery } = require("@google-cloud/bigquery")

const PROJECT_ID = "engaged-ground-326604"
const TOPIC_NAME = "projects/engaged-ground-326604/topics/change-stream"
const SUBSCRIPTION_NAME =
    "projects/engaged-ground-326604/subscriptions/change-stream-sub"
const DATASET_ID = "test"
const TABLE_ID = "orders"

const pubsub = new PubSub({ projectId: PROJECT_ID })
const bigquery = new BigQuery()
const subscription = pubsub.topic(TOPIC_NAME).subscription(SUBSCRIPTION_NAME)

subscription.on("message", async (message) => {
    const doc = JSON.parse(message.data.toString())

    const rows = [
        {
            id: doc._id,
            number: doc.orderNumber,
            sell_status: doc.orderSellStatus,
            serve_status: doc.orderServeStatus,
            type: doc.orderType,
            tax_amount: doc.taxAmount * 100,
            discount_amount: doc.discountAmount * 100,
            delivery_amount: doc.deliveryAmount * 100,
            subtotal_amount: doc.subTotalAmount * 100,
            total_amount: doc.totalAmount * 100,
            store_id: doc.storeId,
            merchant_id: doc.merchantId,
            created_at: doc.createdAt,
            updated_at: doc.updatedAt,
        },
    ]

    console.log(`order ${doc._id} received`)

    await bigquery.dataset(DATASET_ID).table(TABLE_ID).insert(rows)
    message.ack()

    console.log(`order ${doc._id} inserted to bigquery`)
})
