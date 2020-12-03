import {} from '@electricui/build-rollup-config'

import {
  CancellationToken,
  DEVICE_EVENTS,
  Deferred,
  Device,
  Message,
  MessageQueue,
  PipelinePromise,
} from '@electricui/core'

import { MAX_ACK_NUM } from '@electricui/protocol-binary-constants'
import debug from 'debug'

const dQueue = debug('electricui-protocol-binary-fifo-queue:queue')

class QueuedMessage {
  message: Message
  deferreds: Array<Deferred<Message>>
  retries: number = 0
  cancellationToken: CancellationToken

  constructor(message: Message, deferred: Deferred<Message>, cancellationToken: CancellationToken) {
    this.message = message
    this.deferreds = [deferred]
    this.cancellationToken = cancellationToken

    this.addDeferred = this.addDeferred.bind(this)
  }

  addDeferred(deferred: Deferred<Message>) {
    this.deferreds.push(deferred)
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
  /**
   * One shot MessageIDs are never retried
   */
  oneShotMessageIDs?: Array<string>
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
  oneShotMessageIDs: Array<string>

  constructor(options: MessageQueueBinaryFIFOOptions) {
    super(options.device)

    // 0 is an 'acceptable' number for testing, so explicitly check for undefined
    if (typeof options.concurrentMessages === 'undefined') {
      this.concurrentMessages = 100
    } else {
      this.concurrentMessages = options.concurrentMessages
    }

    this.canRoute = this.canRoute.bind(this)
    this.routeMessage = this.routeMessage.bind(this)

    this.onConnect = this.onConnect.bind(this)
    this.onDisconnect = this.onDisconnect.bind(this)
    this.clearQueue = this.clearQueue.bind(this)
    this.decrementMessagesInTransit = this.decrementMessagesInTransit.bind(this)
    this.tick = this.tick.bind(this)

    this.interval = options.interval || 50

    this.retries = options.retries || 3

    this.nonIdempotentMessageIDs = options.nonIdempotentMessageIDs || []

    this.oneShotMessageIDs = options.oneShotMessageIDs ?? []

    // Get notified when the device disconnects from everything
    options.device.on(DEVICE_EVENTS.CONNECTION, this.onConnect)
    options.device.on(DEVICE_EVENTS.DISCONNECTION, this.onDisconnect)
  }

  queue(message: Message, cancellationToken: CancellationToken): PipelinePromise {
    if (!cancellationToken) {
      throw new Error(`Message ${message.messageID} was queued without a CancellationToken`)
    }

    dQueue(`Queuing message`, message.messageID)

    const deferred = new Deferred<Message>()

    // Return a promise that will resolve with the promise of the write, when it writes
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
          dQueue(
            'Deduplicating message in queue',
            "Old message's payload isn't null, overwriting with that payload",
            inQueue.message.payload,
          )
          payload = inQueue.message.payload
        }

        // if the new message isn't null, we then override with that
        if (message.payload !== null) {
          dQueue(
            'Deduplicating message in queue',
            "New message's payload isn't null, overwriting with that payload",
            message.payload,
          )
          payload = message.payload
        }

        // deduplicate this message
        inQueue.addDeferred(deferred)

        // mutate the things we could have changed
        this.messages[index].message.metadata.ack = ack
        this.messages[index].message.metadata.query = query
        this.messages[index].message.payload = payload

        dQueue(`deduplicating message`, message, 'with', this.messages[index].message)
        this.tick()

        return deferred.promise
      }
    } else {
      dQueue(`message is non idempotent`, message.messageID)
    }

    // this message isn't going to be deduplicated

    const queuedMessage = new QueuedMessage(message, deferred, cancellationToken)

    this.messages.push(queuedMessage)

    dQueue(`New message`, message.messageID)

    this.tick()
    return deferred.promise
  }

  clearQueue() {
    for (const msg of this.messages) {
      msg.cancellationToken.cancel()
    }
    this.messages = []
    this.messagesInTransit = 0
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
    return this.device.canRoute()
  }

  async routeMessage(dequeuedMessage: QueuedMessage) {
    // Clone the message
    const clonedMessage = Message.from(dequeuedMessage.message)

    dQueue(`Routing message`, dequeuedMessage, dequeuedMessage.message, dequeuedMessage.message.payload)

    dQueue(`- Queue length: ${this.messages.length}, messages in transit: ${this.messagesInTransit}`)

    try {
      const val = await this.device.route(clonedMessage, dequeuedMessage.cancellationToken)

      for (const deferred of dequeuedMessage.deferreds) {
        deferred.resolve(val)
      }
    } catch (err) {
      // Increment the ackNum if it's an ack message on our copy of it
      if (dequeuedMessage.message.metadata.ack) {
        dequeuedMessage.message.metadata.ackNum += 1
      }

      // increment the retry counter
      dequeuedMessage.retries += 1

      if (
        // If it's a one shot message, don't do any retries
        this.oneShotMessageIDs.includes(dequeuedMessage.message.messageID) ||
        // If it's exceeded the max ack number, fail it out
        dequeuedMessage.message.metadata.ackNum > MAX_ACK_NUM ||
        // If it's exceeded the max retry number, fail it out
        dequeuedMessage.retries > this.retries ||
        // If this was a cancellation, reject up the chain
        dequeuedMessage.cancellationToken.caused(err)
      ) {
        for (const deferred of dequeuedMessage.deferreds) {
          deferred.reject(err)
        }

        return
      }

      // add it back to the queue and try again if we have retries left
      // console.log('retrying message', dequeuedMessage)
      this.messages.unshift(dequeuedMessage)
    }
  }

  decrementMessagesInTransit() {
    this.messagesInTransit -= 1
  }

  /**
   *
   */
  tick() {
    if (this.messages.length === 0) {
      // exit early silently
      return
    }

    dQueue(`Tick Start - Queue length: ${this.messages.length}, messages in transit: ${this.messagesInTransit}`)

    if (!this.canRoute()) {
      dQueue(`Message router reporting that it can't route`)
      return
    }

    // Repeat as long as we have quota for our outgoing messages
    while (this.canRoute() && this.messages.length > 0 && this.messagesInTransit < this.concurrentMessages) {
      const dequeuedMessage = this.messages.shift()!

      // Add to our counter of messages in transit
      this.messagesInTransit += 1

      this.routeMessage(dequeuedMessage)
        .then(this.decrementMessagesInTransit)
        .catch(err => {
          console.warn('Error in FIFO Queue routing')
          throw err
        })
    }
  }
}
