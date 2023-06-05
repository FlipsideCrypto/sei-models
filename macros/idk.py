# !pip install cosmospy-protobuf

import base64
import json
import cosmospy_protobuf.cosmos.tx.v1beta1.tx_pb2 as tx_pb2




tx = "CrIDCq8DCigvc2VpcHJvdG9jb2wuc2VpY2hhaW4uZGV4Lk1zZ1BsYWNlT3JkZXJzEoIDCipzZWkxajh4dXhnNnd6dXBobXBscWp3aDlrZ3BhMzk0aDIzNWo4Z3Q2cDkSzgEqEzEwMDAwMDAwMDAwMDAwMDAwMDAyEzEwMDAwMDAwMDAwMDAwMDAwMDA6N2ZhY3Rvcnkvc2VpMWo4eHV4ZzZ3enVwaG1wbHFqd2g5a2dwYTM5NGgyMzVqOGd0NnA5L3N1c2RCOGZhY3Rvcnkvc2VpMWo4eHV4ZzZ3enVwaG1wbHFqd2g5a2dwYTM5NGgyMzVqOGd0NnA5L3NpbWJhWil7ImxldmVyYWdlIjoiMSIsInBvc2l0aW9uX2VmZmVjdCI6Ik9wZW4ifWoBMHIBMBo+c2VpMXV0aDk1YTRtM3JzZnRwZjQ2a3B6Y3poMnY4OHljOHk4dnFoaHN5cnhsZm1zbDJkNnZ0anNqeXUyMG0iQwo4ZmFjdG9yeS9zZWkxajh4dXhnNnd6dXBobXBscWp3aDlrZ3BhMzk0aDIzNWo4Z3Q2cDkvc2ltYmESBzEwMDAwMDASZQpNCkMKHS9jb3Ntb3MuY3J5cHRvLnNyMjU1MTkuUHViS2V5EiIKIIoXKbhOoUZVmK2FLSYXNq4TpaOMbD5seszobNsdithCEgQKAggBGCcSFAoOCgR1c2VpEgYzMDAwMDAQwJoMGkBe3rxqE6chMjZLZbHS3faXZwJcxEvg+fhxldj/4PUWPGUVffVEcGtZsEIAVa9IKJZHQ87TZBPrOKwFd3UeP6OE"
decode64 = base64.b64decode(tx)
tx = tx_pb2.Tx()
tx.ParseFromString(decode64)


# decode64 = decode64.decode('unicode_escape').encode('utf-8')
# decode64 = decode64.decode('utf-8')
# print(decode64)
# decode64 = decode64.decode('utf-16')
# decode64 = decode64.decode('utf-8')
# fin = str(decode64)

# string = bytes.decode(decode64, 'utf-8')
# str = unicode(decode64, errors='ignore')
# string = decode64.decode('unicode_escape').encode('utf-8')
# print(string)

# decode64 = decode64.rstrip("\n").decode("utf-16")
# decode64 = decode64.split("\r\n")

string = decode64.decode('unicode_escape').encode('utf-8')
print(string)



# print(tx)
# print(tx.body)
# print(tx.body.messages)
# print(tx.auth_info)
# print(tx.signatures)


# dictionary = {'tx':fin}
# jsonString = json.dumps(dictionary, indent=4)
# print(jsonString)

# dictionary = {'body':tx.body, 'auth_info':tx.auth_info, 'signature':tx.signatures}
# jsonString = json.dumps(dictionary, indent=4)
# print(jsonString)



