import { Device, DeviceManager, MessageRouterLogRatioMetadata } from '@electricui/core'

const manager = new DeviceManager()
manager.setCreateRouterCallback(
  device =>
    new MessageRouterLogRatioMetadata({
      device,
    }),
)

const device = new Device('test', manager)
