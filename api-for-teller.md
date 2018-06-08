统一说明 返回json object结构统一为： 成功：{"code":0, "data":object} 失败：{"code":1,"errmsg":"errmsg","data":object}  
  
1. /api/count-available-address/  
获取可用地址数量  
```bash
Method: GET  
Request Body:  
成功：{"code":0,"data": 238}  
失败：{"code":1,"errmsg":"errmsg"}  
```

2. /api/address/  
提交可用地址  
```bash
Method: POST  
Content-Type: application/json  
Request Body:  
[{
    "address": "te3iJRP9XNb4hbavxt4ckjBhy9PHo5a9LM", 
    "checksum": "iaosdfsauasdf"
}, 
{
    "address": "fd3iJRP9XNb4hbavxt4ckjBhy9PHo5a9xa", 
    "checksum": "jasdhuyasudy"
}]
Response:  
成功：{"code":0}  
失败：{"code":1,"errmsg":"errmsg"}  
```

3. /api/deposit/  
发送存入信息，要么整体全部失败，要么整体全部成功  
```bash
Method: POST  
Content-Type: application/json  
Request Body:  
[{
    "seq": 3, 
    "updated_at": 1513210524, 
    "address": "6v7gu8WP2V9aggo", 
    "txid": "3486ca63d6169536c4552bm", 
    "amount": 12000000, 
    "height": 105948
}, 
{
    "seq": 4, 
    "updated_at": 1513220524, 
    "address": "6v7gu8WP2V9aggp", 
    "txid": "3486ca63d6169536c4552bn", 
    "amount": 2000000, 
    "height": 105949
}]
Response:  
成功：{"code":0}  
失败：{"code":1,"errmsg":"errmsg"}  
```
