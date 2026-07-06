"""
Test the exact scenario from the screenshot:
Model outputs <read_file> XML tags in streaming response.
"""
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy import XMLToJSONConverter, parse_xml_tool_calls

# Simulate the exact output from the screenshot
screenshot_output = """I'll help you add a console log before the quotecall response in the single car journey. Let me first locate where the quote API call happens in the single car flow.

<read_file>
<path>src/app/fw/pc-single/pc-single-fwcar-details/pc-single-fwcar-details.component.ts</path>
</read_file>

<read_file>
<path>src/app/fw/pc-single/pc-single.component.ts</path>
</read_file>

Let me search for the quote API call in the globals service:

<read_file>
<path>src/app/fw/service/global.ts</path>
<start_line>1</start_line>
<end_line>200</end_line>
</read_file>"""

print("Testing screenshot scenario...")
print("=" * 80)

# Test 1: Parse the XML directly
print("\n1. Testing parse_xml_tool_calls()...")
tool_calls = parse_xml_tool_calls(screenshot_output)
print(f"Found {len(tool_calls)} tool calls:")
for tc in tool_calls:
    print(f"  - {tc['name']}: {tc['arguments']}")

assert len(tool_calls) == 3, f"Expected 3 tool calls, got {len(tool_calls)}"
assert tool_calls[0]["name"] == "read_file"
assert tool_calls[0]["arguments"]["path"] == "src/app/fw/pc-single/pc-single-fwcar-details/pc-single-fwcar-details.component.ts"
assert tool_calls[1]["name"] == "read_file"
assert tool_calls[1]["arguments"]["path"] == "src/app/fw/pc-single/pc-single.component.ts"
assert tool_calls[2]["name"] == "read_file"
assert tool_calls[2]["arguments"]["path"] == "src/app/fw/service/global.ts"
assert tool_calls[2]["arguments"]["start_line"] == 1
assert tool_calls[2]["arguments"]["end_line"] == 200

print("✓ parse_xml_tool_calls() works correctly")

# Test 2: Test streaming converter
print("\n2. Testing XMLToJSONConverter (streaming)...")
converter = XMLToJSONConverter(is_openai_format=True)

# Simulate streaming by feeding the output in chunks
chunks = screenshot_output.split("\n")
all_text = ""
all_tool_chunks = []

for chunk in chunks:
    text, tool_chunks = converter.process_chunk_text(chunk + "\n")
    all_text += text
    all_tool_chunks.extend(tool_chunks)

print(f"Text yielded: {len(all_text)} chars")
print(f"Tool chunks generated: {len(all_tool_chunks)}")

assert len(all_tool_chunks) == 3, f"Expected 3 tool chunks, got {len(all_tool_chunks)}"
assert all_tool_chunks[0]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"] == "read_file"
assert all_tool_chunks[1]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"] == "read_file"
assert all_tool_chunks[2]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"] == "read_file"

print("✓ XMLToJSONConverter works correctly")

# Test 3: Verify the tool calls have correct arguments
print("\n3. Verifying tool call arguments...")
args0 = json.loads(all_tool_chunks[0]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"])
assert args0["path"] == "src/app/fw/pc-single/pc-single-fwcar-details/pc-single-fwcar-details.component.ts"

args1 = json.loads(all_tool_chunks[1]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"])
assert args1["path"] == "src/app/fw/pc-single/pc-single.component.ts"

args2 = json.loads(all_tool_chunks[2]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"])
assert args2["path"] == "src/app/fw/service/global.ts"
assert args2["start_line"] == 1
assert args2["end_line"] == 200

print("✓ All tool call arguments are correct")

print("\n" + "=" * 80)
print("ALL SCREENSHOT SCENARIO TESTS PASSED!")
print("\nThe proxy should now correctly convert <read_file> XML tags to tool calls.")
print("If it's still not working, the proxy needs to be restarted.")
