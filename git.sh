git reset "$(git merge-base origin/master "$(git branch --show-current)")"
git add -A && git commit -m '对比master分支,合并commit为一条提交记录：修复 cfg 空指针问题。'