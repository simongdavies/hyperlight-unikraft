(component
  (core module $arithmetic
    (func (export "add") (param i32 i32) (result i32)
      local.get 0
      local.get 1
      i32.add))

  (core instance $arithmetic-instance (instantiate $arithmetic))

  (func $add (param "left" s32) (param "right" s32) (result s32)
    (canon lift (core func $arithmetic-instance "add")))

  (export "add" (func $add)))
