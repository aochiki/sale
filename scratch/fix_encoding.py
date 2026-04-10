import codecs

target_file = r'c:\Users\aono\Desktop\antigravity\sales_aggregator\app.py'
output_file = r'c:\Users\aono\Desktop\antigravity\sales_aggregator\app_fixed.py'

encodings = ['utf-8-sig', 'utf-8', 'cp932', 'shift_jis', 'latin1']

for enc in encodings:
    try:
        with codecs.open(target_file, 'r', encoding=enc, errors='strict') as f:
            content = f.read()
            print(f"Success with {enc}")
            # UTF-8 で正規化して一時ファイルに書き出す
            with open(output_file, 'w', encoding='utf-8') as out:
                out.write(content)
            break
    except Exception as e:
        print(f"Failed with {enc}: {e}")
else:
    # どれもダメなら errors='ignore' で強引に読む
    with codecs.open(target_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
        print("Success with utf-8 (errors='ignore')")
        with open(output_file, 'w', encoding='utf-8') as out:
            out.write(content)
