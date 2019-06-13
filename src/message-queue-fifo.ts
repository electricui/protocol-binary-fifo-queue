import {
  deepEqual,
  Device,
  DEVICE_EVENTS,
  Message,
  MessageQueue,
  PipelinePromise,
} from '@electricui/core'
import { MAX_ACK_NUM, TYPES } from '@electricui/protocol-binary-constants'

const dQueue = require('debug')('electricui-protocol-binary-fifo-queue:queue')

type Resolver = (value: any) => void

class QueuedMessage {
  message: Message
  resolves: Array<Resolver>
  rejections: Array<Resolver>

  constructor(message: Message, resolve: Resolver, reject: Resolver) {
    this.message = message
    this.resolves = [resolve]
    this.rejections = [reject]

    this.addResolve = this.addResolve.bind(this)
    this.addReject = this.addReject.bind(this)
  }

  addResolve(resolve: Resolver) {
    this.resolves.push(resolve)
  }

  addReject(reject: Resolver) {
    this.rejections.push(reject)
  }
}

export interface MessageQueueBinaryFIFOOptions {
  device: Device
  concurrentMessages?: number
  interval?: number
  retries?: number
  /**
   * These are messageIDs that cannot be deduplicated while in the queue
   */
  nonIdempotentMessageIDs?: Array<string>
}
/**
 * Holds a device level message queue
 */
export class MessageQueueBinaryFIFO extends MessageQueue {
  messages: Array<QueuedMessage> = []
  concurrentMessages: number
  intervalReference: NodeJS.Timer | null = null
  interval: number
  messagesInTransit: number = 0
  retries: number
  nonIdempotentMessageIDs: Array<string>

  constructor(options: MessageQueueBinaryFIFOOptions) {
    super(options.device)

    // 0 is an 'acceptable' number for testing, so explicitly check for undefined
    if (typeof options.concurrentMessages === 'undefined') {
      this.concurrentMessages = 100
    } else {
      this.concurrentMessages = options.concurrentMessages
    }

    this.canRoute = this.canRoute.bind(this)

    this.onConnect = this.onConnect.bind(this)
    this.onDisconnect = this.onDisconnect.bind(this)
    this.clearQueue = this.clearQueue.bind(this)
    this.tick = this.tick.bind(this)

    this.interval = options.interval || 50

    this.retries = options.retries || 3

    this.nonIdempotentMessageIDs = options.nonIdempotentMessageIDs || []

    // Get notified when the device disconnects from everything
    options.device.on(DEVICE_EVENTS.CONNECTION, this.onConnect)
    options.device.on(DEVICE_EVENTS.DISCONNECTION, this.onDisconnect)
  }

  queue(message: Message): PipelinePromise {
    if (this.device.messageRouter === null) {
      throw new Error('The device needs a messageRouter set')
    }

    dQueue(`Queuing message`, message.messageID)

    // add the retry counter if it doesn't exist yet

    if (typeof message.metadata._retry === 'undefined') {
      message.metadata._retry = 0
    }

    // Return a promise that will resolve with the promise of the write, when it writes
    return new Promise((resolve, reject) => {
      // check if this message is on the list of non-idempotent messageIDs
      if (!this.nonIdempotentMessageIDs.includes(message.messageID)) {
        // iterate over all the messages in the queue
        for (const [index, inQueue] of this.messages.entries()) {
          // check if it has the same messageID, if not, next message
          if (inQueue.message.messageID !== message.messageID) {
            continue
          }

          // if they're namespaced internal / developer differently, continue
          if (inQueue.message.metadata.internal !== message.metadata.internal) {
            continue
          }

          // if the types are different, continue
          if (inQueue.message.metadata.type !== message.metadata.type) {
            continue
          }

          // if inQueue has an offset that's different to this offset, continue
          if (inQueue.message.metadata.offset !== message.metadata.offset) {
            continue
          }

          // if inQueue has an ackNum that's different to this ackNum, continue
          if (inQueue.message.metadata.ackNum !== message.metadata.ackNum) {
            continue
          }

          // if one has an ack and one doesn't, we CAN deduplicate
          // we go 'true' unless both are false
          const ack = inQueue.message.metadata.ack || message.metadata.ack

          // if one is a query and one isn't a query, we CAN deduplicate
          const query = inQueue.message.metadata.query || message.metadata.query

          // build the payload
          let payload = null

          // if the old message isn't null, we override with that
          if (inQueue.message.payload !== null) {
            payload = inQueue.message.payload
          }

          // if the new message isn't null, we then override with that
          if (message.payload !== null) {
            payload = message.payload
          }

          // deduplicate this message
          inQueue.addResolve(resolve)
          inQueue.addReject(reject)

          // mutate the things we could have changed
          this.messages[index].message.metadata.ack = ack
          this.messages[index].message.metadata.query = query
          this.messages[index].message.payload = payload

          dQueue(`deduplicating message`, message)
          this.tick()
          return
        }
      } else {
        dQueue(`message is non idempotent`, message.messageID)
      }

      // this message isn't goign to be deduplicated

      const queuedMessage = new QueuedMessage(message, resolve, reject)

      this.messages.push(queuedMessage)

      dQueue(`New message`, message.messageID)

      this.tick()
      return
    })
  }

  clearQueue() {
    for (const msg of this.messages) {
      for (const resolve of msg.resolves) {
        // Reject all promises with a disconnection message
        resolve(Promise.reject('Device Disconnected'))
      }
    }
    this.messages = []
  }

  onConnect() {
    this.intervalReference = setInterval(this.tick, this.interval)

    dQueue(`Spinning up queue loop`)

    // clear the queue.
    this.clearQueue()

    dQueue(`Device connected, queue setup complete`)
  }

  onDisconnect() {
    if (this.intervalReference) {
      clearInterval(this.intervalReference)
    }

    dQueue(`Spinning down queue loop`)

    // clear the queue
    this.clearQueue()

    dQueue(`Queue teardown complete`)
  }

  canRoute() {
    return this.device.messageRouter!.canRoute()
  }

  /**
   *
   */
  tick() {
    if (this.messages.length === 0) {
      // exit early silently
      return
    }

    dQueue(
      `Tick Start - Queue length: ${
        this.messages.length
      }, messages in transit: ${this.messagesInTransit}`,
    )

    if (!this.canRoute()) {
      dQueue(`Message router reporting that it can't route`)
      return
    }

    // Repeat as long as we have quota for our outgoing messages
    while (
      this.canRoute() &&
      this.messages.length > 0 &&
      this.messagesInTransit < this.concurrentMessages
    ) {
      const msg = this.messages.shift()!

      // Add to our counter of messages in transit
      this.messagesInTransit += 1

      this.device
        .messageRouter!.route(msg.message)
        .then(val => {
          // call all the resolvers
          for (const resolve of msg.resolves) {
            resolve(val)
          }
        })
        .catch(err => {
          console.error('Message failed', err, msg.message)

          // Increment the ackNum if it's an ack message
          if (msg.message.metadata.ack) {
            msg.message.metadata.ackNum += 1
          }

          // increment the retry counter
          msg.message.metadata._retry += 1

          // If it's exceeded the max ack number, fail it out
          if (msg.message.metadata.ackNum > MAX_ACK_NUM) {
            for (const reject of msg.rejections) {
              reject(err)
            }

            return
          }

          // If it's exceeded the max retry number, fail it out
          if (msg.message.metadata._retry > this.retries) {
            for (const reject of msg.rejections) {
              reject(err)
            }

            return
          }

          // add it back to the queue and try again if we have retries left
          console.log('retrying message', msg)
          this.messages.unshift(msg)
        })
        .finally(() => {
          // A message is no longer in transit, reduce the count
          this.messagesInTransit -= 1
        })
    }
  }
}

// return
