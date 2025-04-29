/*  sjdb/Optimiser.java  –  greedy join by input-product first  */
package sjdb;

import java.util.*;
import java.util.function.BiConsumer;

public class Optimiser {

    private final Estimator est = new Estimator();

    public Optimiser(Catalogue cat) {}

    public Operator optimise(Operator canonical) {
        /* ── 0. 拆掉顶层 Project ── */
        List<Attribute> topProj = null;
        if (canonical instanceof Project) {
            topProj  = ((Project) canonical).getAttributes();
            canonical = ((Project) canonical).getInput();
        }

        /* ── 1. 收集 Scan 与谓词 ── */
        Info info = new Info();
        collect(canonical, info);

        /* ── 2. 若不是 SELECT * ，计算每表需要的列 ── */
        boolean starSelect = (topProj == null);               // SELECT * ?
        Map<String, Set<Attribute>> need = new HashMap<>();   // rel → attrs

        if (!starSelect) {
            // 想用的助手
            BiConsumer<Attribute,Void> add = (attr, v) -> {
                for (Scan s : info.scans)
                    if (hasAttr(s.getRelation(), attr)) {
                        need.computeIfAbsent(s.getRelation().toString(),
                                        k -> new LinkedHashSet<>())
                                .add(attr);
                        break;
                    }
            };

            // 2.1 顶层投影
            for (Attribute a : topProj) add.accept(a,null);
            // 2.2 所有谓词
            for (Predicate p : info.eqPreds) {
                add.accept(p.getLeftAttribute(),null);
                add.accept(p.getRightAttribute(),null);
            }
            for (Predicate p : info.restPreds) {
                add.accept(p.getLeftAttribute(),null);
            }
        }

        /* ── 3. 构造 base map，并在 Scan 上方插 Project(裁列) ── */
        Map<String,Operator> base = new LinkedHashMap<>();
        for (Scan s : info.scans) {
            String rel = s.getRelation().toString();
            Operator op = s;

            if (!starSelect) {
                Set<Attribute> keep = need.get(rel);
                if (keep != null &&
                        keep.size() < s.getRelation().getAttributes().size()) {

                    // 维持列顺序：按 catalogue 中出现的先后
                    List<Attribute> ordered = new ArrayList<>();
                    for (Attribute a : s.getRelation().getAttributes())
                        if (keep.contains(a)) ordered.add(a);

                    op = new Project(s, ordered);   // 列裁剪
                }
            }
            base.put(rel, op);
        }

        /* ── 起点：行数最小的 Scan ── */
        Operator leftTree = pickMinScan(base);
        Set<String> joined = new HashSet<>();
        joined.add(scanName(leftTree));

        /* ── 贪婪地把其余表接到左边 ── */
        while (joined.size() < base.size()) {

            JoinChoice best = null;

            for (String rel : base.keySet()) if (!joined.contains(rel)) {

                Operator right = base.get(rel);
                Predicate pred = peekPred(leftTree, right, info.eqPreds);

                Operator cand = (pred == null)
                        ? new Product(leftTree, right)
                        : new Join   (leftTree, right, pred);

                cand.accept(est);
                int outRows = cand.getOutput().getTupleCount();
                int leftRows  = leftTree.getOutput().getTupleCount();
                int rightRows = right    .getOutput().getTupleCount();
                long inProduct = (long) leftRows * rightRows;

                if (best == null ||
                        inProduct <  best.inProd ||
                        (inProduct == best.inProd && outRows < best.outRows))
                    best = new JoinChoice(rel, cand, pred, inProduct, outRows);
            }

            /* 用掉已选谓词 */
            if (best.pred != null) info.eqPreds.remove(best.pred);

            leftTree = best.plan;
            joined.add(best.rel);
        }

        /* ── 剩余谓词作为 Select ── */
        for (Predicate p : info.restPreds)
            leftTree = new Select(leftTree, p);

        return (topProj == null) ? leftTree
                : new Project(leftTree, topProj);
    }

    /* ====== 收集阶段 ====== */
    private static class Info {
        final List<Scan> scans = new ArrayList<>();
        final List<Predicate> eqPreds   = new ArrayList<>();
        final List<Predicate> restPreds = new ArrayList<>();
    }

    private void collect(Operator op, Info I) {
        if (op instanceof Scan) {
            I.scans.add((Scan) op);
        } else if (op instanceof Select) {
            Predicate p = ((Select) op).getPredicate();
            if (p.equalsValue()) I.restPreds.add(p);
            else                 I.eqPreds  .add(p);
            collect(((Select) op).getInput(), I);
        } else if (op instanceof Project) {
            collect(((Project) op).getInput(), I);
        } else if (op instanceof BinaryOperator) {
            collect(((BinaryOperator) op).getLeft(),  I);
            collect(((BinaryOperator) op).getRight(), I);
        }
    }

    /* ====== 找到一条连接 left/right 的谓词（只窥视不删除） ====== */
    private Predicate peekPred(Operator L, Operator R, List<Predicate> list){
        for (Predicate p : list)
            if (connects(L,R,p)) return p;
        return null;
    }

    private boolean connects(Operator L, Operator R, Predicate p){
        Attribute a = p.getLeftAttribute(), b = p.getRightAttribute();
        return (belongs(L,a)&&belongs(R,b)) || (belongs(L,b)&&belongs(R,a));
    }

    private boolean belongs(Operator tree, Attribute x){
        if (tree instanceof Scan) return hasAttr(((Scan)tree).getRelation(),x);
        if (tree instanceof Select)  return belongs(((Select) tree).getInput(), x);
        if (tree instanceof Project) return belongs(((Project)tree).getInput(), x);
        if (tree instanceof BinaryOperator)
            return belongs(((BinaryOperator) tree).getLeft(),x) ||
                    belongs(((BinaryOperator) tree).getRight(),x);
        return false;
    }
    private boolean hasAttr(Relation r, Attribute x){
        for (Attribute a : r.getAttributes())
            if (a.equals(x)) return true;
        return false;
    }

    /* ====== 选最小 Scan 作为根 ====== */
    private Operator pickMinScan(Map<String,Operator> base){
        Operator best=null; int min=Integer.MAX_VALUE;
        for (Operator op:base.values()){
            op.accept(est);
            int n=op.getOutput().getTupleCount();
            if (n<min){min=n; best=op;}
        }
        return best;
    }
    private String scanName(Operator op){
        if (op instanceof Scan) return ((Scan)op).getRelation().toString();
        if (op instanceof Select)  return scanName(((Select) op).getInput());
        if (op instanceof Project) return scanName(((Project)op).getInput());
        throw new IllegalStateException();
    }

    /* ====== 保存一次备选结果 ====== */
    private static class JoinChoice{
        final String rel; final Operator plan; final Predicate pred;
        final long inProd; final long outRows;
        JoinChoice(String r,Operator p,Predicate pr,long in,long out){
            rel=r; plan=p; pred=pr; inProd=in; outRows=out;
        }
    }
}
