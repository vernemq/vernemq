salt = bcrypt.gen_salt()
hash = bcrypt.hashpw("my-password", salt)
assert(bcrypt.hashpw("my-password", hash) == hash)
