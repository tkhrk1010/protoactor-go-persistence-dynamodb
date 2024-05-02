#!/bin/bash

#
# 型と関数の定義を抽出するスクリプト
# LLMに食べてもらうと、型を覚えてくれるのでcodeingの相談がしやすい。
# 全文貼るとtoken数が多くなりすぎるので、型と関数の定義だけを抽出して出力し、渡す。
#

# 出力ファイル名
output_file="types_and_funcs.txt"

# 出力ファイルを空にする
> "$output_file"

# すべての .go ファイルを再帰的に処理する (persistence ディレクトリ、パスに test が含まれるディレクトリ・ファイル、_test を含むファイルは除外)
find . -path ".././persistence/" -prune -o -path "*/test/*" -prune -o -name "*_test*" -prune -o -name "*.go" -print0 | while IFS= read -r -d '' file; do
    # 型と関数の定義を抽出し、出力ファイルに追加する
    awk '
        /^type/ || /^func/ {
            print $0
        }
    ' "$file" >> "$output_file"
done

echo "型と関数の定義が $output_file に出力されました。"