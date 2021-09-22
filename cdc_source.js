const { MongoClient } = require("mongodb")
const { PubSub } = require("@google-cloud/pubsub")

const PROJECT_ID = "engaged-ground-326604"
const TOPIC_NAME = "projects/engaged-ground-326604/topics/change-stream"
const ORDERS_DB_URI =
    "mongodb+srv://eodevstore:eodevstore@poc-7aaiu.mongodb.net/dev?retryWrites=true&w=majority"
const ORDERS_COLLECTION_NAME = "orders"
const ORDERS_DB_NAME = "dev"

const client = new MongoClient(ORDERS_DB_URI)
const pubsub = new PubSub({ projectId: PROJECT_ID })
const topic = pubsub.topic(TOPIC_NAME)

async function main() {
    await client.connect()

    const db = client.db(ORDERS_DB_NAME)
    const collection = db.collection(ORDERS_COLLECTION_NAME)

    const changeStream = registerChangeStream(collection)

    await closeChangeStream(1e8, changeStream)
}

function registerChangeStream(collection) {
    const changeStream = collection.watch({ fullDocument: "updateLookup" })

    changeStream.on("change", (next) => {
        const message = JSON.stringify(next.fullDocument)

        topic.publish(Buffer.from(message))

        console.log(`order ${next.fullDocument._id} published`)
    })

    changeStream.on("error", console.error)

    return changeStream
}

function closeChangeStream(timeInMs = 60000, changeStream) {
    return new Promise((resolve) => {
        setTimeout(() => {
            console.log("Closing the change stream")

            changeStream.close()

            resolve()
        }, timeInMs)
    })
}

main()
    .then(console.log)
    .catch(console.error)
    .finally(() => client.close())
