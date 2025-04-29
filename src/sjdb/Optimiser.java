/*  sjdb/Optimiser.java  –  heuristic left-deep optimiser (2025-04)  */
package sjdb;

import java.util.*;
import java.util.function.BiConsumer;

public class Optimiser {

    private final Estimator est = new Estimator();

    public Optimiser(Catalogue cat) {}

    /* ─────────────────────────── 主流程 ─────────────────────────── */

    public Operator optimise(Operator canonical) {

        /* 0. 拆掉顶层 Project（若有） */
        List<Attribute> topProj = null;
        if (canonical instanceof Project) {
            topProj  = ((Project) canonical).getAttributes();
            canonical = ((Project) canonical).getInput();
        }

        /* 1. 收集所有 Scan 与 Select 谓词 */
        Info info = new Info();
        collect(canonical, info);

        /* 2. 计算各表真正需要保留的列 */
        boolean starQuery = (topProj == null);
        Map<String, Set<Attribute>> need = computeNeed(info, topProj, starQuery);

        /* 3. 为每张表构造 leaf（常量选择下推 + 必要列裁剪） */
        Map<String,Operator> base = new LinkedHashMap<>();
        Map<String,Integer>  origSize = new HashMap<>();      // 原始行数
        buildLeaves(info, need, starQuery, base, origSize);

        /* 4. 选择根表（原始行数最小） */
        Operator leftTree = pickRoot(base, origSize);
        Set<String> joined = new HashSet<>();
        joined.add(scanName(leftTree));

        /* 5. 贪婪扩展 + 1-step look-ahead */
        while (joined.size() < base.size()) {
            JoinChoice best = chooseNext(leftTree, joined, base, info);
            if (best.pred != null) info.eqPreds.remove(best.pred);
            leftTree = best.plan;
            joined.add(best.rel);
        }

        /* 6. 把剩余 attr=value 谓词挂在顶端 */
        for (Predicate p : info.restPreds)
            leftTree = new Select(leftTree, p);

        /* 7. 恢复顶层 Project；若已冗余则省掉 */
        if (topProj == null) return leftTree;
        if (leftTree instanceof Project &&
                sameAttr(((Project) leftTree).getAttributes(), topProj))
            return leftTree;
        return new Project(leftTree, topProj);
    }

    /* ────────────────── 步骤 2：列需求计算 ────────────────── */

    private Map<String, Set<Attribute>> computeNeed(Info info,
                                                    List<Attribute> topProj,
                                                    boolean star) {
        Map<String, Set<Attribute>> need = new HashMap<>();
        if (star) return need;

        BiConsumer<Attribute,Void> add = (attr, v) -> {
            for (Scan s : info.scans)
                if (hasAttr(s.getRelation(), attr)) {
                    need.computeIfAbsent(s.getRelation().toString(),
                                    k -> new LinkedHashSet<>())
                            .add(attr);
                    break;
                }
        };

        if (topProj != null)
            for (Attribute a : topProj) add.accept(a, null);

        for (Predicate p : info.eqPreds) {
            add.accept(p.getLeftAttribute(),  null);
            add.accept(p.getRightAttribute(), null);
        }
        for (Predicate p : info.restPreds)
            add.accept(p.getLeftAttribute(), null);

        return need;
    }

    /* ─────────── 步骤 3：构造各表 leaf（含 Project 去冗余）────────── */

    private void buildLeaves(Info info,
                             Map<String, Set<Attribute>> need,
                             boolean star,
                             Map<String,Operator> base,
                             Map<String,Integer> origSize) {

        for (Scan s : info.scans) {
            String rel = s.getRelation().toString();
            Operator op = s;
            origSize.put(rel, s.getRelation().getTupleCount());

            /* 3.1 下推 attr=value 谓词，记录只此处用到的列 */
            Set<Attribute> consumed = new HashSet<>();
            for (Iterator<Predicate> it = info.restPreds.iterator(); it.hasNext();) {
                Predicate p = it.next();
                Attribute a = p.getLeftAttribute();
                if (p.equalsValue() && hasAttr(s.getRelation(), a)) {
                    op = new Select(op, p);
                    it.remove();
                    consumed.add(a);
                }
            }

            /* 3.2 如需列裁剪则添加 Project */
            if (!star) {
                Set<Attribute> keep = need.get(rel);
                if (keep != null) {
                    /* 把“只在已下推谓词里用过”的列剔除 */
                    for (Attribute a : new ArrayList<>(keep))
                        if (consumed.contains(a) &&
                                !appearsElsewhere(a, null, info.eqPreds, info.restPreds))
                            keep.remove(a);

                    if (keep.size() < s.getRelation().getAttributes().size()) {
                        List<Attribute> ordered = new ArrayList<>();
                        for (Attribute a : s.getRelation().getAttributes())
                            if (keep.contains(a)) ordered.add(a);
                        op = new Project(op, ordered);
                    }
                }
            }
            base.put(rel, op);
        }
    }

    /* ──────────── 选根表：原始行数最小 ──────────── */

    private Operator pickRoot(Map<String,Operator> base,
                              Map<String,Integer> origSize) {
        String best = null; int min = Integer.MAX_VALUE;
        for (String r : base.keySet()) {
            int sz = origSize.get(r);
            if (sz < min) { min = sz; best = r; }
        }
        return base.get(best);
    }

    /* ───────── 选择下一张表（含 look-ahead）───────── */

    private JoinChoice chooseNext(Operator leftTree,
                                  Set<String> joined,
                                  Map<String,Operator> base,
                                  Info info) {

        JoinChoice best = null;

        for (String rel : base.keySet()) if (!joined.contains(rel)) {

            Operator right = base.get(rel);
            Predicate pred = peekPred(leftTree, right, info.eqPreds);

            Operator cand = (pred == null)
                    ? new Product(leftTree, right)
                    : new Join   (leftTree, right, pred);

            cand.accept(est);
            int outRows   = cand.getOutput().getTupleCount();
            int leftRows  = leftTree.getOutput().getTupleCount();
            int rightRows = right   .getOutput().getTupleCount();
            long inProd   = (long) leftRows * rightRows;

            /* 1-step look-ahead */
            int finalRows = outRows;
            if (base.size() - joined.size() - 1 == 1) {           // 只剩最后一表
                Operator last = null;
                for (String r2 : base.keySet())
                    if (!joined.contains(r2) && !r2.equals(rel))
                        last = base.get(r2);

                Predicate p2 = peekPred(cand, last, info.eqPreds);
                Operator end = (p2 == null)
                        ? new Product(cand, last)
                        : new Join   (cand, last, p2);
                end.accept(est);
                finalRows = end.getOutput().getTupleCount();
            }

            /* 选择规则：
               1) finalRows 小者优先
               2) 若相等 → inProd **大者**优先（避免先连小表导致基数放大）
               3) 仍相等 → outRows 小者优先                              */
            if (best == null ||
                    finalRows <  best.finalRows ||
                    (finalRows == best.finalRows && inProd   > best.inProd) ||
                    (finalRows == best.finalRows && inProd   == best.inProd && outRows < best.outRows))
                best = new JoinChoice(rel, cand, pred, inProd, outRows, finalRows);
        }
        return best;
    }

    /* ──────────────────── 工具 / 内部类 ──────────────────── */

    /** 某列在剩余结构中是否仍被使用？(顶层投影传 null 表示已考虑过) */
    private boolean appearsElsewhere(Attribute a,
                                     List<Attribute> topProj,
                                     List<Predicate> eqs,
                                     List<Predicate> rests) {
        if (topProj != null && topProj.contains(a)) return true;
        for (Predicate p : eqs)
            if (a.equals(p.getLeftAttribute()) || a.equals(p.getRightAttribute()))
                return true;
        for (Predicate p : rests)
            if (a.equals(p.getLeftAttribute())) return true;
        return false;
    }

    /** 比较两投影列集合是否完全相等（忽略顺序） */
    private boolean sameAttr(List<Attribute> a, List<Attribute> b){
        return new HashSet<>(a).equals(new HashSet<>(b));
    }

    /* ========= 关系/属性辅助 ========= */

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
        if (tree instanceof Scan)       return hasAttr(((Scan)tree).getRelation(), x);
        if (tree instanceof Select)     return belongs(((Select)tree).getInput(), x);
        if (tree instanceof Project)    return belongs(((Project)tree).getInput(), x);
        if (tree instanceof BinaryOperator)
            return belongs(((BinaryOperator)tree).getLeft(),  x) ||
                    belongs(((BinaryOperator)tree).getRight(), x);
        return false;
    }
    private boolean hasAttr(Relation r, Attribute x){
        for (Attribute a : r.getAttributes())
            if (a.equals(x)) return true;
        return false;
    }
    private String scanName(Operator op){
        if (op instanceof Scan)    return ((Scan)op).getRelation().toString();
        if (op instanceof Select)  return scanName(((Select)  op).getInput());
        if (op instanceof Project) return scanName(((Project) op).getInput());
        throw new IllegalStateException();
    }

    /* ========= 收集信息 ========= */

    private static class Info {
        final List<Scan>      scans     = new ArrayList<>();
        final List<Predicate> eqPreds   = new ArrayList<>();  // attr=attr
        final List<Predicate> restPreds = new ArrayList<>();  // attr=value
    }
    private void collect(Operator op, Info I) {
        if (op instanceof Scan) {
            I.scans.add((Scan) op);
        } else if (op instanceof Select) {
            Predicate p = ((Select) op).getPredicate();
            (p.equalsValue() ? I.restPreds : I.eqPreds).add(p);
            collect(((Select) op).getInput(), I);
        } else if (op instanceof Project) {
            collect(((Project) op).getInput(), I);
        } else if (op instanceof BinaryOperator) {
            collect(((BinaryOperator) op).getLeft(),  I);
            collect(((BinaryOperator) op).getRight(), I);
        }
    }

    /* ========= JoinChoice 记录 ========= */

    private static class JoinChoice {
        final String    rel;
        final Operator  plan;
        final Predicate pred;
        final long      inProd;
        final long      outRows;
        final long      finalRows;
        JoinChoice(String r, Operator p, Predicate pr,
                   long in, long out, long fin){
            rel = r; plan = p; pred = pr;
            inProd = in; outRows = out; finalRows = fin;
        }
    }
}
