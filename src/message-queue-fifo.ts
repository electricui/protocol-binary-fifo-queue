import {} from '@electricui/build-rollup-config'

import { CancellationToken, Deferred, UnanimousCancellationToken } from '@electricui/async-utilities'

import { CONNECTION_STATE, DEVICE_EVENTS, Device, Message, MessageQueue, PipelinePromise } from '@electricui/core'

import { MAX_ACK_NUM } from '@electricui/protocol-binary-constants'
import debug from 'debug'

const dQueue = debug('electricui-protocol-binary-fifo-queue:queue')

class QueuedMessage {
  message: Message
  deferreds: Array<{
    deferred: Deferred<void>
    cancellationToken: CancellationToken
  }> = []
  retries: number = 0
  unanimousCancellationToken: UnanimousCancellationToken = new UnanimousCancellationToken()

  constructor(message: Message) {
    this.message = message
    this.addDeferred = this.addDeferred.bind(this)
  }

  addDeferred(deferred: Deferred<void>, cancellationToken: CancellationToken) {
    this.deferreds.push({ deferred, cancellationToken })
    this.unanimousCancellationToken.addToken(cancellationToken)
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
  messagesInTransit: Set<QueuedMessage> = new Set()
  retries: number
  nonIdempotentMessageIDs: Array<string>
  oneShotMessageIDs: Array<string>

  constructor(options: MessageQueueBinaryFIFOOptions) {
    super(options.device)

    if (options.concurrentMessages === 0) {
      throw new Error("Can't create a FIFO queue with a maximum of 0 concurrent messages")
    }

    if (typeof options.concurrentMessages === 'undefined') {
      this.concurrentMessages = 100
    } else {
      this.concurrentMessages = options.concurrentMessages
    }

    this.canRoute = this.canRoute.bind(this)
    this.routeMessage = this.routeMessage.bind(this)

    this.deviceConnectionChange = this.deviceConnectionChange.bind(this)
    this.startup = this.startup.bind(this)
    this.teardown = this.teardown.bind(this)
    this._pause = this._pause.bind(this)
    this._resume = this._resume.bind(this)
    this.clearQueue = this.clearQueue.bind(this)
    this.tick = this.tick.bind(this)

    this.interval = options.interval || 50

    this.retries = options.retries || 3

    this.nonIdempotentMessageIDs = options.nonIdempotentMessageIDs || []

    this.oneShotMessageIDs = options.oneShotMessageIDs ?? []

    // Get notified when the device disconnects from everything
    options.device.on(DEVICE_EVENTS.AGGREGATE_CONNECTION_STATE_CHANGE, this.deviceConnectionChange)
  }

  public queue(message: Message, cancellationToken: CancellationToken): PipelinePromise {
    if (!cancellationToken) {
      throw new Error(`Message ${message.messageID} was queued without a CancellationToken`)
    }

    dQueue(`Queuing message ${message.messageID} with ${this.messages.length} in the queue.`)

    const deferred = new Deferred<void>()

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
        inQueue.addDeferred(deferred, cancellationToken)

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

    // Copy the cancellation token since we can't cancel the upper one on disconnection,
    // since we don't own it

    const queuedMessage = new QueuedMessage(message)
    queuedMessage.addDeferred(deferred, cancellationToken)

    // If the message is cancelled, remove it from the messagesInTransit set
    queuedMessage.unanimousCancellationToken.getToken().subscribe(() => {
      this.messagesInTransit.delete(queuedMessage)
      this.messages = this.messages.filter(msg => msg !== queuedMessage)
    })

    this.messages.push(queuedMessage)

    dQueue(`New message`, message.messageID)

    this.tick()
    return deferred.promise
  }

  clearQueue() {
    // Cancel any outgoing messageIDs
    for (let index = 0; index < this.messages.length; index++) {
      const message = this.messages[index]

      // Reject each message with its own cancellation token
      for (const deferred of message.deferreds) {
        deferred.deferred.reject(deferred.cancellationToken.token)
      }

      // Cancel the unanimousCancellationToken
      message.unanimousCancellationToken.getToken().cancel()
    }

    // Cancel any messages in flight
    for (const message of this.messagesInTransit) {
      // Reject each message with its own cancellation token
      for (const deferred of message.deferreds) {
        deferred.deferred.reject(deferred.cancellationToken.token)
      }

      // Cancel the unanimousCancellationToken
      message.unanimousCancellationToken.getToken().cancel()
    }

    // this should already be 0
    if (this.messages.length !== 0) {
      this.messages.length = 0
    }

    // this should already be 0
    if (this.messagesInTransit.size !== 0) {
      this.messagesInTransit.clear()
    }
  }

  deviceConnectionChange(device: Device, state: CONNECTION_STATE) {
    if (state === CONNECTION_STATE.CONNECTING) {
      this.startup()
      return
    }

    if (state === CONNECTION_STATE.DISCONNECTING) {
      this.teardown()
      return
    }
  }

  startup() {
    if (this.intervalReference) {
      clearInterval(this.intervalReference)
    }
    this.intervalReference = setInterval(this.tick, this.interval)

    dQueue(`Spinning up queue loop`)

    // clear the queue.
    this.clearQueue()

    dQueue(`Device connected, queue setup complete`)
  }

  teardown() {
    if (this.intervalReference) {
      clearInterval(this.intervalReference)
      this.intervalReference = null
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

    dQueue(`- Queue length: ${this.messages.length}, messages in transit: ${this.messagesInTransit.size}`)

    try {
      await this.device.route(clonedMessage, dequeuedMessage.unanimousCancellationToken.getToken())

      // Reduce the messages in transit
      this.messagesInTransit.delete(dequeuedMessage)

      // Resolve each of the promises
      for (const deferred of dequeuedMessage.deferreds) {
        deferred.deferred.resolve()
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
        dequeuedMessage.unanimousCancellationToken.getToken().caused(err)
      ) {
        // If this was a cancellation, reject up the chain
        if (dequeuedMessage.unanimousCancellationToken.getToken().caused(err)) {
          for (const deferred of dequeuedMessage.deferreds) {
            deferred.deferred.reject(deferred.cancellationToken.token)
          }
        } else {
          // Otherwise pass the same error up the chain
          for (const deferred of dequeuedMessage.deferreds) {
            deferred.deferred.reject(err)
          }
        }

        console.error(
          `Message ${dequeuedMessage.message.messageID} failed to send ${dequeuedMessage.retries} ${
            dequeuedMessage.retries === 1 ? 'time' : 'times'
          }. No longer retrying. Error:`,
          err,
        )

        return
      }

      console.warn(
        `Message ${dequeuedMessage.message.messageID} failed to send ${dequeuedMessage.retries} ${
          dequeuedMessage.retries === 1 ? 'time' : 'times'
        }. Retrying. Error:`,
        err,
      )

      // add it back to the queue and try again if we have retries left
      this.messages.unshift(dequeuedMessage)
    }
  }

  private paused = false

  public _pause() {
    this.paused = true
  }

  public _resume() {
    this.paused = false
  }

  /**
   *
   */
  tick() {
    if (this.messages.length === 0 || this.paused) {
      // exit early silently
      return
    }

    dQueue(`Tick Start - Queue length: ${this.messages.length}, messages in transit: ${this.messagesInTransit.size}`)

    if (!this.canRoute()) {
      dQueue(`Message router reporting that it can't route`)
      return
    }

    // Repeat as long as we have quota for our outgoing messages
    while (this.canRoute() && this.messages.length > 0 && this.messagesInTransit.size < this.concurrentMessages) {
      const dequeuedMessage = this.messages.shift()!

      // Add to our counter of messages in transit
      this.messagesInTransit.add(dequeuedMessage)

      this.routeMessage(dequeuedMessage).catch(err => {
        console.warn('Error in FIFO Queue routing')
        throw err
      })
    }
  }
}
