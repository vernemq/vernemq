use core
db.createUser( { user: "vmq_auth_source_test_user",
                 pwd: "vmq_auth_source_test_password",
                 roles: ["readWrite"] },
               { w: "majority" , wtimeout: 5000 } )
