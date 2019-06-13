import 'mocha'

import * as chai from 'chai'
import * as sinon from 'sinon'
import { EventEmitter } from 'events'
import {
  Device,
  DEVICE_EVENTS,
  MessageRouterTestCallback,
  Message,
  PipelinePromise,
  MessageRouter,
} from '@electricui/core'

import {
  MessageQueueBinaryFIFO,
  MessageQueueBinaryFIFOOptions,
} from '../src/message-queue-fifo'

const assert = chai.assert

/**
 * This mock device implements the minimum methods to test the queue
 *
 * To write at it, call queue.queue(new Message())
 *
 * it will "write" over the callback
 */
class MockDevice extends EventEmitter {
  messageRouter: MessageRouter
  messageQueue?: MessageQueueBinaryFIFO
  isConnected: boolean = false

  constructor(callback: (message: Message) => PipelinePromise) {
    super()
    this.messageRouter = new MessageRouterTestCallback(
      (this as unknown) as Device,
      callback,
    )
  }

  connect = () => {
    this.isConnected = true
    this.emit(DEVICE_EVENTS.CONNECTION)
  }

  disconnect = () => {
    this.isConnected = false
    this.emit(DEVICE_EVENTS.DISCONNECTION)
  }

  write = (message: Message) => {
    return this.messageQueue!.queue(message).catch(err => {
      // console.log(err)
    })
  }
}

function createQueueTestFixtures(
  options: Omit<MessageQueueBinaryFIFOOptions, 'device'>,
) {
  const spy = sinon.spy()
  const callback = (message: Message) => {
    // process this 'next tick'
    setImmediate(() => {
      spy(message)
    })

    return Promise.resolve()
  }
  const device = new MockDevice(callback)

  const queue = new MessageQueueBinaryFIFO({
    device: (device as unknown) as Device,
    ...options,
  })

  queue.canRoute = () => device.isConnected

  return {
    spy,
    queue,
    device,
  }
}

function delay(timeout: number) {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, timeout)
  })
}

describe('MessageQueueBinaryFIFO', () => {
  it('we can trick the queue into sending nothing by setting the concurrent message count to 0', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
    })

    // console.log('Queue concurrentMessages', queue.concurrentMessages)

    device.connect()
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
    device.write(new Message('a', 4))
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
    device.write(new Message('b', 4))
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
    device.write(new Message('c', 4))
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)

    assert.deepEqual(spy.callCount, 0, 'We should have receive 0 messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 3, 'There should be 3 messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'The messages in transit should be 0') // prettier-ignore

    device.disconnect()

    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
  })

  it('we can step through the queue in variable batches', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
    })

    // console.log('Queue concurrentMessages', queue.concurrentMessages)

    device.connect()
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
    device.write(new Message('a', 4))
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
    device.write(new Message('b', 4))
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
    device.write(new Message('c', 4))
    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)

    // Before any ticks, there should be 3 in the queue
    assert.deepEqual(spy.callCount, 0, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 3, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    queue.concurrentMessages = 1
    queue.tick()
    assert.deepEqual(spy.callCount, 0, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 2, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 1, 'Should be this many messages in transit') // prettier-ignore
    await delay(10)
    assert.deepEqual(spy.callCount, 1, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 2, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    queue.concurrentMessages = 2

    queue.tick()
    assert.deepEqual(spy.callCount, 1, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 2, 'Should be this many messages in transit') // prettier-ignore
    await delay(10)
    assert.deepEqual(spy.callCount, 3, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    device.disconnect()

    // console.log(spy.callCount, queue.messages.length, queue.messagesInTransit)
  })

  it('clears the buffer on connect and disconnect', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 5,
    })

    device.write(new Message('a', 4))
    // console.log('connecting')
    device.connect()
    // console.log('connected, this clears the queue')
    device.write(new Message('b', 4))
    device.write(new Message('c', 4))

    device.write(new Message('d', 4))
    device.write(new Message('e', 4))

    device.disconnect()
    device.write(new Message('f', 4))

    // let the promises fulfil
    await delay(10)

    assert.deepEqual(spy.callCount, 4, 'We should have receive 4 messages') // should have been called 4 times
    assert.deepEqual(
      queue.messages.length,
      1,
      'There should be one message left in the queue at the end',
    ) // there should be one message left in the queue
    assert.deepEqual(
      queue.messagesInTransit,
      0,
      'The messages in transit should be 0',
    ) // and zero in transit

    assert.deepEqual((spy.getCall(0).args[0] as Message).messageID, 'b')
    assert.deepEqual((spy.getCall(1).args[0] as Message).messageID, 'c')
    assert.deepEqual((spy.getCall(2).args[0] as Message).messageID, 'd')
    assert.deepEqual((spy.getCall(3).args[0] as Message).messageID, 'e')
  })

  it('consecutive messages with the same messageID are deduplicated and the correct payload is sent when flushed', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
    })

    device.connect()
    device.write(new Message('a', 1))
    device.write(new Message('a', 2))
    device.write(new Message('a', 3))

    queue.concurrentMessages = 100
    queue.tick()
    await delay(10)
    assert.deepEqual(spy.callCount, 1, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    assert.deepEqual((spy.getCall(0).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(0).args[0] as Message).payload, 3)

    device.disconnect()
  })

  it('non-consecutive messages that are not marked as non-idempotent with the same messageID are deduplicated and the correct payload is sent when flushed', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
    })

    device.connect()
    device.write(new Message('a', 1))
    device.write(new Message('b', 2))
    device.write(new Message('a', 3))
    device.write(new Message('b', 4))

    queue.concurrentMessages = 100
    queue.tick()
    await delay(10)
    assert.deepEqual(spy.callCount, 2, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    assert.deepEqual((spy.getCall(0).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(0).args[0] as Message).payload, 3)

    assert.deepEqual((spy.getCall(1).args[0] as Message).messageID, 'b')
    assert.deepEqual((spy.getCall(1).args[0] as Message).payload, 4)

    device.disconnect()
  })

  it('a query+write and a raw query should be deduplicated correctly', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
    })

    const queryWriteMessage = new Message('a', null)
    queryWriteMessage.metadata.query = true

    device.connect()
    device.write(queryWriteMessage)
    device.write(new Message('a', 3))

    queue.concurrentMessages = 100
    queue.tick()
    await delay(10)
    assert.deepEqual(spy.callCount, 1, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    assert.deepEqual((spy.getCall(0).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(0).args[0] as Message).payload, 3)
    assert.deepEqual((spy.getCall(0).args[0] as Message).metadata.query, true)

    device.disconnect()
  })

  it('a raw query and a query+write should be deduplicated correctly', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
    })

    const queryWriteMessage = new Message('a', null)
    queryWriteMessage.metadata.query = true

    device.connect()
    device.write(new Message('a', 3))
    device.write(queryWriteMessage)

    queue.concurrentMessages = 100
    queue.tick()
    await delay(10)
    assert.deepEqual(spy.callCount, 1, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    assert.deepEqual((spy.getCall(0).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(0).args[0] as Message).payload, 3)
    assert.deepEqual((spy.getCall(0).args[0] as Message).metadata.query, true)

    device.disconnect()
  })

  it('pure queries for the same messageID are deduplicated', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
    })

    const queryWriteMessage = new Message('a', null)
    queryWriteMessage.metadata.query = true

    device.connect()
    device.write(queryWriteMessage)
    device.write(queryWriteMessage)

    queue.concurrentMessages = 100
    queue.tick()
    await delay(10)
    assert.deepEqual(spy.callCount, 1, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    assert.deepEqual((spy.getCall(0).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(0).args[0] as Message).payload, null)
    assert.deepEqual((spy.getCall(0).args[0] as Message).metadata.query, true)

    device.disconnect()
  })

  it('pure queries for the same messageID but at different offsets are not deduplicated', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
    })

    const queryWriteMessage1 = new Message('a', null)
    queryWriteMessage1.metadata.query = true
    queryWriteMessage1.metadata.offset = 0

    const queryWriteMessage2 = new Message('a', null)
    queryWriteMessage2.metadata.query = true
    queryWriteMessage2.metadata.offset = 100

    device.connect()
    device.write(queryWriteMessage1)
    device.write(new Message('b', 2))
    device.write(queryWriteMessage2)
    device.write(new Message('b', 4))

    queue.concurrentMessages = 100
    queue.tick()
    await delay(10)
    assert.deepEqual(spy.callCount, 3, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    assert.deepEqual((spy.getCall(0).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(0).args[0] as Message).payload, null)
    assert.deepEqual((spy.getCall(0).args[0] as Message).metadata.offset, 0)

    assert.deepEqual((spy.getCall(1).args[0] as Message).messageID, 'b')
    assert.deepEqual((spy.getCall(1).args[0] as Message).payload, 4)

    assert.deepEqual((spy.getCall(2).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(2).args[0] as Message).payload, null)
    assert.deepEqual((spy.getCall(2).args[0] as Message).metadata.offset, 100)

    device.disconnect()
  })

  it('writes for messageIDs that are marked as non-idempotent are not deduplicated', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
      nonIdempotentMessageIDs: ['a'],
    })

    device.connect()
    device.write(new Message('a', 1))
    device.write(new Message('a', 2))
    device.write(new Message('b', 1))
    device.write(new Message('b', 2))

    queue.concurrentMessages = 100
    queue.tick()
    await delay(10)
    assert.deepEqual(spy.callCount, 3, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    assert.deepEqual((spy.getCall(0).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(0).args[0] as Message).payload, 1)

    assert.deepEqual((spy.getCall(1).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(1).args[0] as Message).payload, 2)

    assert.deepEqual((spy.getCall(2).args[0] as Message).messageID, 'b')
    assert.deepEqual((spy.getCall(2).args[0] as Message).payload, 2)

    device.disconnect()
  })

  it('queries for messageIDs that are marked as non-idempotent are not deduplicated', async () => {
    const { spy, queue, device } = createQueueTestFixtures({
      concurrentMessages: 0,
      nonIdempotentMessageIDs: ['a'],
    })

    const queryWriteMessage1 = new Message('a', null)
    queryWriteMessage1.metadata.query = true

    const queryWriteMessage2 = new Message('a', null)
    queryWriteMessage2.metadata.query = true

    device.connect()
    device.write(queryWriteMessage1)
    device.write(queryWriteMessage2)

    queue.concurrentMessages = 100
    queue.tick()
    await delay(10)
    assert.deepEqual(spy.callCount, 2, 'Should have received this many messages') // prettier-ignore
    assert.deepEqual(queue.messages.length, 0, 'Should be this many messages left in the queue') // prettier-ignore
    assert.deepEqual(queue.messagesInTransit, 0, 'Should be this many messages in transit') // prettier-ignore

    assert.deepEqual((spy.getCall(0).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(0).args[0] as Message).payload, null)
    assert.deepEqual((spy.getCall(0).args[0] as Message).metadata.query, true)

    assert.deepEqual((spy.getCall(1).args[0] as Message).messageID, 'a')
    assert.deepEqual((spy.getCall(1).args[0] as Message).payload, null)
    assert.deepEqual((spy.getCall(1).args[0] as Message).metadata.query, true)

    device.disconnect()
  })
})
