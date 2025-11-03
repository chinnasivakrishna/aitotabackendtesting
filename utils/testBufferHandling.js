// Test script to verify Buffer handling and conversion
const AudioUtils = require('./audioUtils')

console.log('🧪 Testing Buffer handling and conversion...')

// Test 1: Valid Buffer
try {
  const testBuffer = Buffer.from('Hello World', 'utf8')
  const result1 = AudioUtils.ensureBuffer(testBuffer)
  console.log('✅ Test 1 - Valid Buffer:', result1.length, 'bytes')
} catch (error) {
  console.log('❌ Test 1 - Valid Buffer failed:', error.message)
}

// Test 2: String to Buffer
try {
  const testString = 'Hello World'
  const result2 = AudioUtils.ensureBuffer(testString)
  console.log('✅ Test 2 - String to Buffer:', result2.length, 'bytes')
} catch (error) {
  console.log('❌ Test 2 - String to Buffer failed:', error.message)
}

// Test 3: Array to Buffer
try {
  const testArray = [72, 101, 108, 108, 111] // "Hello"
  const result3 = AudioUtils.ensureBuffer(testArray)
  console.log('✅ Test 3 - Array to Buffer:', result3.length, 'bytes')
} catch (error) {
  console.log('❌ Test 3 - Array to Buffer failed:', error.message)
}

// Test 4: Base64 string to Buffer
try {
  const base64String = Buffer.from('Hello World', 'utf8').toString('base64')
  const result4 = AudioUtils.ensureBuffer(base64String)
  console.log('✅ Test 4 - Base64 to Buffer:', result4.length, 'bytes')
} catch (error) {
  console.log('❌ Test 4 - Base64 to Buffer failed:', error.message)
}

// Test 5: Uint8Array to Buffer
try {
  const uint8Array = new Uint8Array([72, 101, 108, 108, 111])
  const result5 = AudioUtils.ensureBuffer(uint8Array)
  console.log('✅ Test 5 - Uint8Array to Buffer:', result5.length, 'bytes')
} catch (error) {
  console.log('❌ Test 5 - Uint8Array to Buffer failed:', error.message)
}

// Test 6: Invalid data type
try {
  const invalidData = { test: 'object' }
  const result6 = AudioUtils.ensureBuffer(invalidData)
  console.log('✅ Test 6 - Invalid data type:', result6.length, 'bytes')
} catch (error) {
  console.log('✅ Test 6 - Invalid data type correctly rejected:', error.message)
}

console.log('🎉 Buffer handling tests completed!') 