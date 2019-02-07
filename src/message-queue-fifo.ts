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

  constructor(message: Message, resolve: Resolver) {
    this.message = message
    this.resolves = [resolve]

    this.addResolve = this.addResolve.bind(this)
  }

  addResolve(resolve: Resolver) {
    this.resolves.push(resolve)
  }
}

interface MessageQueueBinaryFIFOOptions {
  device: Device
  concurrentMessages?: number
  interval?: number
  retries?: number
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

  constructor(options: MessageQueueBinaryFIFOOptions) {
    super(options.device)

    this.concurrentMessages = options.concurrentMessages || 100

    this.canRoute = this.canRoute.bind(this)

    this.onConnect = this.onConnect.bind(this)
    this.onDisconnect = this.onDisconnect.bind(this)
    this.clearQueue = this.clearQueue.bind(this)
    this.tick = this.tick.bind(this)

    this.interval = options.interval || 50

    // Get notified when the device disconnects from everything
    options.device.on(DEVICE_EVENTS.CONNECTION, this.onConnect)
    options.device.on(DEVICE_EVENTS.DISCONNECTION, this.onDisconnect)
  }

  queue(message: Message): PipelinePromise {
    if (this.device.messageRouter === null) {
      throw new Error('The device needs a messageRouter set')
    }

    // Return a promise that will resolve with the promise of the write, when it writes
    return new Promise((resolve, reject) => {
      // Check if the current packet matches the previous one
      const lastInQueue: QueuedMessage | undefined = this.messages[
        this.messages.length - 1
      ]

      // Check if we can deduplicate the packet
      if (
        lastInQueue !== undefined &&
        lastInQueue.message.messageID === message.messageID &&
        message.metadata.type !== TYPES.CALLBACK &&
        deepEqual(lastInQueue.message.metadata, message.metadata)
      ) {
        // deduplicate
        lastInQueue.addResolve(resolve)
      } else {
        // new packet

        const queuedMessage = new QueuedMessage(message, resolve)

        this.messages.push(queuedMessage)
      }
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

    // clear the queue.
    this.clearQueue()

    dQueue(`Device connected, queue setup complete`)
  }

  onDisconnect() {
    if (this.intervalReference) {
      clearInterval(this.intervalReference)
    }

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

          // A message is no longer in transit, reduce the count
          this.messagesInTransit -= 1
        })
        .catch(err => {
          console.error('Message failed', err, msg.message)

          // Increment the ackNum, it's our retry counter
          msg.message.metadata.ackNum += 1

          // A message is no longer in transit, reduce the count
          this.messagesInTransit -= 1

          // If it's exceeded the max ack number, fail it out
          if (msg.message.metadata.ackNum > MAX_ACK_NUM) {
            for (const resolve of msg.resolves) {
              resolve(Promise.reject(err))
            }

            return
          }

          // add it back to the queue and try again if we have retries left
          this.messages.unshift(msg)
        })
    }

    dQueue(
      `Tick Complete - Queue length: ${
        this.messages.length
      }, messages in transit: ${this.messagesInTransit}`,
    )
  }
}

// return
