package sjdb;

import java.io.*;
import java.util.ArrayList;
import sjdb.DatabaseException;

public class Test2 {
    public static void main(String[] args) throws Exception {
        // 创建包含三个关系的 Catalogue
        Catalogue cat = createCatalogue();
        Inspector inspector = new Inspector();
        Estimator estimator = new Estimator();

        // 构造复杂查询计划
        Operator plan = queryComplex(cat);
        // 使用访问者模式进行代价估算
        plan.accept(estimator);
        // 使用 Inspector 打印各层的统计信息
        plan.accept(inspector);


        // 看看优化过后的结果！
        Optimiser optimiser = new Optimiser(cat);
        Operator planopt = optimiser.optimise(plan);
        planopt.accept(estimator);
        planopt.accept(inspector);
    }

    /**
     * 创建一个包含关系 A、B、C 的 Catalogue
     */
    public static Catalogue createCatalogue() {
        Catalogue cat = new Catalogue();
        // 关系 A：100 行，属性 a1 (100 distinct), a2 (15 distinct)
        cat.createRelation("A", 100);
        cat.createAttribute("A", "a1", 100);
        cat.createAttribute("A", "a2", 15);

        // 关系 B：150 行，属性 b1 (150 distinct), b2 (100 distinct), b3 (5 distinct)
        cat.createRelation("B", 150);
        cat.createAttribute("B", "b1", 150);
        cat.createAttribute("B", "b2", 100);
        cat.createAttribute("B", "b3", 5);

        // 关系 C：200 行，属性 c1 (200 distinct), c2 (50 distinct)
        cat.createRelation("C", 200);
        cat.createAttribute("C", "c1", 200);
        cat.createAttribute("C", "c2", 50);

        return cat;
    }

    /**
     * 构造复杂查询计划：
     * 语义上模拟以下 SQL 查询：
     *
     *   SELECT a2 = b3 AND b2 = c1 AND a1 = "const"
     *   FROM A, B, C
     *   PROJECT [a2, b1, c2]
     *
     * 其中：
     *   - 第一个选择：a2 = b3（连接关系 A 和 B）
     *   - 第二个选择：b2 = c1（连接关系 B 和 C）
     *   - 第三个选择：a1 = "const"（常量选择，针对关系 A中的属性）
     *
     * 构造的树结构（自底向上）：
     *
     *               Project [a2, b1, c2]
     *                         |
     *                 Select [a1 = "const"]
     *                         |
     *                 Select [b2 = c1]
     *                         |
     *                 Select [a2 = b3]
     *                         |
     *          Product ( (A TIMES B) TIMES C )
     *                     /                    \
     *            Product (A TIMES B)          Scan(C)
     *                 /         \
     *           Scan(A)       Scan(B)
     */
    public static Operator queryComplex(Catalogue cat) throws Exception {
        // 构造各个 Scan 算子
        Scan scanA = new Scan(cat.getRelation("A"));
        Scan scanB = new Scan(cat.getRelation("B"));
        Scan scanC = new Scan(cat.getRelation("C"));

        // 构造 Product：先将 A 与 B 连接
        Product prod1 = new Product(scanA, scanB);
        // 再与 C 进行 Product，形成左深树
        Product prod2 = new Product(prod1, scanC);

        // 构造链式选择
        // 第一个 Select：选择谓词 a2 = b3（作用于 A 和 B）
        Select select1 = new Select(prod2, new Predicate(new Attribute("a2"), new Attribute("b3")));
        // 第二个 Select：选择谓词 b2 = c1（作用于 B 和 C）
        Select select2 = new Select(select1, new Predicate(new Attribute("b2"), new Attribute("c1")));
        // 第三个 Select：选择谓词 a1 = "const"（常量选择，作用于 A）
        Select select3 = new Select(select2, new Predicate(new Attribute("a1"), "const"));

        // 最后构造 Project 操作，仅保留属性 [a2, b1, c2]
        ArrayList<Attribute> projAttrs = new ArrayList<Attribute>();
        projAttrs.add(new Attribute("a2"));
        projAttrs.add(new Attribute("b1"));
        projAttrs.add(new Attribute("c2"));
        Project project = new Project(select3, projAttrs);

        return project;
    }
}
