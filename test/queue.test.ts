import { describe, expect, it, xit, beforeEach, afterEach } from '@jest/globals'
import * as sinon from 'sinon'

import {
  CONNECTION_EVENTS,
  CONNECTION_STATE,
  CancellationToken,
  DEVICE_EVENTS,
  Device,
  Message,
  MessageRouter,
  MessageRouterTestCallback,
  PipelinePromise,
} from '@electricui/core'
import { MessageQueueBinaryFIFO, MessageQueueBinaryFIFOOptions } from '../src/message-queue-fifo'

import { EventEmitter } from 'events'

import FakeTimers from '@sinonjs/fake-timers'

const delay = async (delay: number) => {
  await clock.nextAsync()
  await clock.tickAsync(delay)
}

/**
 * This mock device implements the minimum methods to test the queue
 *
 * To write at it, call queue.queue(new Message())
 *
 * it will "write" over the callback
 */
class MockDevice extends EventEmitter {
  messageRouter: MessageRouter
  messageQueue!: MessageQueueBinaryFIFO
  isConnected: boolean = false

  constructor(callback: (message: Message, cancellationToken: CancellationToken) => PipelinePromise) {
    super()
    this.messageRouter = new MessageRouterTestCallback(this as unknown as Device, callback)
  }

  connect = () => {
    this.isConnected = true
    this.emit(DEVICE_EVENTS.AGGREGATE_CONNECTION_STATE_CHANGE, this, CONNECTION_STATE.CONNECTING)
    this.emit(DEVICE_EVENTS.AGGREGATE_CONNECTION_STATE_CHANGE, this, CONNECTION_STATE.CONNECTED)
  }

  disconnect = () => {
    this.isConnected = false
    this.emit(DEVICE_EVENTS.AGGREGATE_CONNECTION_STATE_CHANGE, this, CONNECTION_STATE.DISCONNECTING)
    this.emit(DEVICE_EVENTS.AGGREGATE_CONNECTION_STATE_CHANGE, this, CONNECTION_STATE.DISCONNECTED)
  }

  canRoute = () => {
    return this.messageRouter.canRoute()
  }

  route = (message: Message, cancellationToken: CancellationToken) => {
    return this.messageRouter.route(message, cancellationToken)
  }

  write = (message: Message, cancellationToken: CancellationToken) => {
    return this.messageQueue.queue(message, cancellationToken)
  }
}

function createQueueTestFixtures(options: Omit<MessageQueueBinaryFIFOOptions, 'device'>) {
  const spy = sinon.spy()
  const callback = (message: Message, cancellationToken: CancellationToken) => {
    return new Promise<void>((resolve, reject) => {
      // process this 'next tick'
      setImmediate(() => {
        if (cancellationToken.isCancelled()) {
          reject(cancellationToken.token)
          return
        }

        spy(message)
        resolve()
      })
    })
  }
  const device = new MockDevice(callback)

  const queue = new MessageQueueBinaryFIFO({
    device: device as unknown as Device,
    ...options,
  })

  device.messageQueue = queue

  queue.canRoute = () => device.isConnected

  return {
    spy,
    queue,
    device,
  }
}

let clock: FakeTimers.Clock

describe('MessageQueueBinaryFIFO', () => {
  beforeEach(() => {
    clock = FakeTimers.install({
      shouldAdvanceTime: true,
      advanceTimeDelta: 20,
    })
  })

  afterEach(() => {
    clock.uninstall()
  })

  it('throws if created with 0 concurrent messages', async () => {
    expect(() =>
      createQueueTestFixtures({
        concurrentMessages: 0,
      }),
    ).toThrow("Can't create a FIFO queue with a maximum of 0 concurrent messages")
  })

  it('the queue can be stepped through in batches', async () => {
    const { spy, queue, device } = createQueueTestFixtures({})
    queue._pause()

    // console.log('Queue concurrentMessages', queue.concurrentMessages)

    device.connect()
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
    device.write(new Message('a', 4), new CancellationToken())
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
    device.write(new Message('b', 4), new CancellationToken())
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
    device.write(new Message('c', 4), new CancellationToken())
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)

    // Before any ticks, there should be 3 in the queue
    expect(spy.callCount).toEqual(0) // prettier-ignore
    expect(queue.messages.length).toEqual(3) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    queue._resume()
    queue.concurrentMessages = 1
    queue.tick()
    queue._pause()
    await delay(50)

    expect(spy.callCount).toEqual(1) // prettier-ignore
    expect(queue.messages.length).toEqual(2) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    queue._resume()
    queue.concurrentMessages = 2
    queue.tick()
    queue._pause()
    await delay(50)

    expect(spy.callCount).toEqual(3) // prettier-ignore
    expect(queue.messages.length).toEqual(0) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    device.disconnect()
  })

  it('clears the buffer on connect and disconnect', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 5,
    })

    try {
      device.write(new Message('a', 4), new CancellationToken())
    } catch (e) {
      console.log('a', e)
    }
    // console.log('connecting')
    device.connect()

    try {
      // console.log('connected, this clears the queue')
      device.write(new Message('b', 4), new CancellationToken())
      device.write(new Message('c', 4), new CancellationToken())

      device.write(new Message('d', 4), new CancellationToken())
      device.write(new Message('e', 4), new CancellationToken())
    } catch (e) {
      console.log('b-d', e)
    }

    await delay(100)
    device.disconnect()

    try {
      device.write(new Message('f', 4), new CancellationToken())
    } catch (e) {}
    // let the promises fulfil
    await delay(100)

    expect(spy.callCount).toEqual(4) // should have been called 4 times
    expect(queue.messages.length).toEqual(1) // there should be one message left in the queue, injected after disconnect
    expect(queue.messagesInTransit.size).toEqual(0) // and zero in transit

    expect((spy.getCall(0).args[0] as Message).messageID).toEqual('b')
    expect((spy.getCall(1).args[0] as Message).messageID).toEqual('c')
    expect((spy.getCall(2).args[0] as Message).messageID).toEqual('d')
    expect((spy.getCall(3).args[0] as Message).messageID).toEqual('e')
  })

  it('consecutive messages with the same messageID are deduplicated and the correct payload is sent when flushed', async () => {
    const { spy, queue, device } = createQueueTestFixtures({})
    queue._pause()

    device.connect()
    device.write(new Message('a', 1), new CancellationToken())
    device.write(new Message('a', 2), new CancellationToken())
    device.write(new Message('a', 3), new CancellationToken())

    queue._resume()

    await delay(10)
    expect(spy.callCount).toEqual(1) // prettier-ignore
    expect(queue.messages.length).toEqual(0) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    expect((spy.getCall(0).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(0).args[0] as Message).payload).toEqual(3)

    device.disconnect()
  })

  it('non-consecutive messages that are not marked as non-idempotent with the same messageID are deduplicated and the correct payload is sent when flushed', async () => {
    const { spy, queue, device } = createQueueTestFixtures({})
    queue._pause()

    device.connect()
    device.write(new Message('a', 1), new CancellationToken())
    device.write(new Message('b', 2), new CancellationToken())
    device.write(new Message('a', 3), new CancellationToken())
    device.write(new Message('b', 4), new CancellationToken())

    queue._resume()

    await delay(10)
    expect(spy.callCount).toEqual(2) // prettier-ignore
    expect(queue.messages.length).toEqual(0) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    expect((spy.getCall(0).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(0).args[0] as Message).payload).toEqual(3)

    expect((spy.getCall(1).args[0] as Message).messageID).toEqual('b')
    expect((spy.getCall(1).args[0] as Message).payload).toEqual(4)

    device.disconnect()
  })

  it('a query+write and a raw query should be deduplicated correctly', async () => {
    const { spy, queue, device } = createQueueTestFixtures({})
    queue._pause()

    const queryWriteMessage = new Message('a', null)
    queryWriteMessage.metadata.query = true

    device.connect()
    device.write(queryWriteMessage, new CancellationToken())
    device.write(new Message('a', 3), new CancellationToken())

    queue._resume()

    await delay(10)
    expect(spy.callCount).toEqual(1) // prettier-ignore
    expect(queue.messages.length).toEqual(0) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    expect((spy.getCall(0).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(0).args[0] as Message).payload).toEqual(3)
    expect((spy.getCall(0).args[0] as Message).metadata.query).toEqual(true)

    device.disconnect()
  })

  it('a raw query and a query+write should be deduplicated correctly', async () => {
    const { spy, queue, device } = createQueueTestFixtures({})
    queue._pause()

    const queryWriteMessage = new Message('a', null)
    queryWriteMessage.metadata.query = true

    device.connect()
    device.write(new Message('a', 3), new CancellationToken())
    device.write(queryWriteMessage, new CancellationToken())

    queue._resume()

    await delay(10)
    expect(spy.callCount).toEqual(1) // prettier-ignore
    expect(queue.messages.length).toEqual(0) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    expect((spy.getCall(0).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(0).args[0] as Message).payload).toEqual(3)
    expect((spy.getCall(0).args[0] as Message).metadata.query).toEqual(true)

    device.disconnect()
  })

  it('pure queries for the same messageID are deduplicated', async () => {
    const { spy, queue, device } = createQueueTestFixtures({})
    queue._pause()

    const queryWriteMessage = new Message('a', null)
    queryWriteMessage.metadata.query = true

    device.connect()
    device.write(queryWriteMessage, new CancellationToken())
    device.write(queryWriteMessage, new CancellationToken())

    queue._resume()

    await delay(10)
    expect(spy.callCount).toEqual(1) // prettier-ignore
    expect(queue.messages.length).toEqual(0) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    expect((spy.getCall(0).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(0).args[0] as Message).payload).toEqual(null)
    expect((spy.getCall(0).args[0] as Message).metadata.query).toEqual(true)

    device.disconnect()
  })

  it('pure queries for the same messageID but at different offsets are not deduplicated', async () => {
    const { spy, queue, device } = createQueueTestFixtures({})
    queue._pause()

    const queryWriteMessage1 = new Message('a', null)
    queryWriteMessage1.metadata.query = true
    queryWriteMessage1.metadata.offset = 0

    const queryWriteMessage2 = new Message('a', null)
    queryWriteMessage2.metadata.query = true
    queryWriteMessage2.metadata.offset = 100

    device.connect()
    device.write(queryWriteMessage1, new CancellationToken())
    device.write(new Message('b', 2), new CancellationToken())
    device.write(queryWriteMessage2, new CancellationToken())
    device.write(new Message('b', 4), new CancellationToken())

    queue._resume()

    await delay(10)
    expect(spy.callCount).toEqual(3) // prettier-ignore
    expect(queue.messages.length).toEqual(0) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    expect((spy.getCall(0).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(0).args[0] as Message).payload).toEqual(null)
    expect((spy.getCall(0).args[0] as Message).metadata.offset).toEqual(0)

    expect((spy.getCall(1).args[0] as Message).messageID).toEqual('b')
    expect((spy.getCall(1).args[0] as Message).payload).toEqual(4)

    expect((spy.getCall(2).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(2).args[0] as Message).payload).toEqual(null)
    expect((spy.getCall(2).args[0] as Message).metadata.offset).toEqual(100)

    device.disconnect()
  })

  it('writes for messageIDs that are marked as non-idempotent are not deduplicated', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      nonIdempotentMessageIDs: ['a'],
    })

    queue._pause()
    device.connect()
    device.write(new Message('a', 1), new CancellationToken())
    device.write(new Message('a', 2), new CancellationToken())
    device.write(new Message('b', 1), new CancellationToken())
    device.write(new Message('b', 2), new CancellationToken())

    queue._resume()
    await delay(10)
    expect(spy.callCount).toEqual(3) // prettier-ignore
    expect(queue.messages.length).toEqual(0) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    expect((spy.getCall(0).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(0).args[0] as Message).payload).toEqual(1)

    expect((spy.getCall(1).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(1).args[0] as Message).payload).toEqual(2)

    expect((spy.getCall(2).args[0] as Message).messageID).toEqual('b')
    expect((spy.getCall(2).args[0] as Message).payload).toEqual(2)

    device.disconnect()
  })

  it('queries for messageIDs that are marked as non-idempotent are not deduplicated', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      nonIdempotentMessageIDs: ['a'],
    })
    queue._pause()

    const queryWriteMessage1 = new Message('a', null)
    queryWriteMessage1.metadata.query = true

    const queryWriteMessage2 = new Message('a', null)
    queryWriteMessage2.metadata.query = true

    device.connect()
    device.write(queryWriteMessage1, new CancellationToken())
    device.write(queryWriteMessage2, new CancellationToken())

    queue._resume()

    await delay(10)
    expect(spy.callCount).toEqual(2) // prettier-ignore
    expect(queue.messages.length).toEqual(0) // prettier-ignore
    expect(queue.messagesInTransit.size).toEqual(0) // prettier-ignore

    expect((spy.getCall(0).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(0).args[0] as Message).payload).toEqual(null)
    expect((spy.getCall(0).args[0] as Message).metadata.query).toEqual(true)

    expect((spy.getCall(1).args[0] as Message).messageID).toEqual('a')
    expect((spy.getCall(1).args[0] as Message).payload).toEqual(null)
    expect((spy.getCall(1).args[0] as Message).metadata.query).toEqual(true)

    device.disconnect()
  })
})
