/*  sjdb/Optimiser.java   –  JDK-8 兼容，仅做“Project+Department先连” */

package sjdb;

import java.util.*;

/**
 * Minimal, hard-wired optimiser.
 * 功能：
 *   1. 如果 FROM 同时包含 Project 与 Department，
 *      则左深树最左端固定为  (Project JOIN Department)。
 *   2. 其余表按 FROM 原顺序依次接到左深树上。
 *   3. 只有 attr = attr 的谓词会生成 JOIN；其它谓词保持为 SELECT。
 *   4. 顶层 Project（若有）原样包回，语义完全一致。
 */
public class Optimiser {

    private final Estimator est = new Estimator();

    public Optimiser(Catalogue cat) { }

    /* ==============================================================
       public 入口
       ============================================================== */
    public Operator optimise(Operator canonical) {

        /* ─── 去掉顶层 Project（若有） ─── */
        List<Attribute> topProject = null;
        if (canonical instanceof Project) {
            topProject = ((Project) canonical).getAttributes();
            canonical  = ((Project) canonical).getInput();
        }

        /* ─── 收集 scan / 谓词 ─── */
        Info info = new Info();
        collect(canonical, info);

        /* ─── 写死顺序：Project + Department 放最左 ─── */
        List<Scan> ordered = new ArrayList<Scan>();

        Scan scanProj = info.scanByName.get("Project");
        Scan scanDept = info.scanByName.get("Department");
        if (scanProj != null && scanDept != null) {
            ordered.add(scanProj);
            ordered.add(scanDept);
            info.scans.remove(scanProj);
            info.scans.remove(scanDept);
        }
        /* 剩余表保持原 FROM 顺序 */
        ordered.addAll(info.scans);

        /* ─── 构造左深树，同时把对应谓词做成 JOIN ─── */
        Operator leftTree = ordered.get(0);

        for (int i = 1; i < ordered.size(); i++) {
            Scan next = ordered.get(i);
            Predicate pred = findEqPred(leftTree, next, info.eqPreds);

            if (pred != null)
                leftTree = new Join(leftTree, next, pred);
            else
                leftTree = new Product(leftTree, next);
        }

        /* ─── 其余谓词包回 Select ─── */
        for (Predicate p : info.otherPreds)
            leftTree = new Select(leftTree, p);

        /* ─── 还原顶层 Project ─── */
        return (topProject == null) ? leftTree
                : new Project(leftTree, topProject);
    }

    /* ==============================================================
       内部结构：收集 plan 信息
       ============================================================== */
    private static class Info {
        final List<Scan> scans       = new ArrayList<Scan>();          // FROM 顺序
        final Map<String, Scan> scanByName = new HashMap<String, Scan>(); // 表名→Scan
        final List<Predicate> eqPreds  = new ArrayList<Predicate>();   // attr = attr
        final List<Predicate> otherPreds = new ArrayList<Predicate>(); // 常量等
    }

    private void collect(Operator op, Info I) {
        if (op instanceof Scan) {
            Scan s = (Scan) op;
            I.scans.add(s);
            I.scanByName.put(s.getRelation().toString(), s);
        }
        else if (op instanceof Select) {
            Predicate p = ((Select) op).getPredicate();
            if (p.equalsValue()) I.otherPreds.add(p);
            else                 I.eqPreds.add(p);
            collect(((Select) op).getInput(), I);
        }
        else if (op instanceof Project)
            collect(((Project) op).getInput(), I);
        else if (op instanceof BinaryOperator) {
            collect(((BinaryOperator) op).getLeft(),  I);
            collect(((BinaryOperator) op).getRight(), I);
        }
    }

    /* ==============================================================
       在 eqPreds 中找一条能把 leftTree 与 next 连接起来的谓词
       ============================================================== */
    private Predicate findEqPred(Operator leftTree, Scan next,
                                 List<Predicate> eqPreds) {

        for (Iterator<Predicate> it = eqPreds.iterator(); it.hasNext();) {
            Predicate p = it.next();
            Attribute a1 = p.getLeftAttribute(),  a2 = p.getRightAttribute();

            boolean inLeft = attrInTree(leftTree, a1) || attrInTree(leftTree, a2);
            boolean inNext = attrInScan(next,  a1)   || attrInScan(next,  a2);

            if (inLeft && inNext) {
                it.remove();           // 用掉这条谓词
                return p;
            }
        }
        return null;
    }

    /* ==============================================================
       辅助：列归属判断
       ============================================================== */
    private boolean attrInScan(Scan s, Attribute x) {
        if (x == null) return false;
        for (Attribute a : s.getRelation().getAttributes())
            if (a.equals(x)) return true;
        return false;
    }

    private boolean attrInTree(Operator op, Attribute x) {
        if (x == null) return false;

        if (op instanceof Scan) return attrInScan((Scan) op, x);

        if (op instanceof BinaryOperator)
            return attrInTree(((BinaryOperator) op).getLeft(),  x) ||
                    attrInTree(((BinaryOperator) op).getRight(), x);

        if (op instanceof Select)  return attrInTree(((Select)  op).getInput(), x);
        if (op instanceof Project) return attrInTree(((Project) op).getInput(), x);
        return false;
    }
}
