
import asyncio

from model.m_handler import BaseHandler


class AuthAccount(BaseHandler):

    @asyncio.coroutine
    def run(self, conn, request):

        conn.write_json(request[r'msgid'], {r'result': r'OK'})


# auth (token, )
# 1. 根据token获取account_info
# 2. connection 绑定 account_info
# 3. 返回 {'account_info': account_info}

# ::客户端连接成功

# create_room (room_option, )
# 1. 根据room_option创建一个room_id, room_json对象
# 2. 将room_json对象 以 room_id 为 key 保存到redis
# 3. 监听 room_id 事件通道
# 3. connection 绑定 房间号
# 4. 返回 {'room_info': room_json}

# ::客户端创建房间成功

# prepare ()
# 1. 根据connection上的房间号，获取redis上的room_json
# 2. 修改room_json中自己的状态为已准备
# 3. 写入到redis
# 4. 通知 room_id 总线有新消息

