package sjdb;

import java.io.*;
import java.util.ArrayList;
import sjdb.DatabaseException;

public class TestInvalidPlan {
    public static void main(String[] args) {
        try {
            // 创建Catalogue，只包含关系A
            Catalogue cat = createCatalogue();
            Inspector inspector = new Inspector();
            Estimator estimator = new Estimator();

            // 构造一个无效查询计划：投影一个不存在的属性 "a3"
            Operator plan = queryInvalid(cat);
            // 运行估算，会抛异常
            plan.accept(estimator);
            plan.accept(inspector);
        } catch (Exception e) {
            System.err.println("Caught exception: " + e.getMessage());
        }
    }

    public static Catalogue createCatalogue() {
        Catalogue cat = new Catalogue();
        // 关系 A: 100 行，属性 a1 (100 distinct), a2 (15 distinct)
        cat.createRelation("A", 100);
        cat.createAttribute("A", "a1", 100);
        cat.createAttribute("A", "a2", 15);
        return cat;
    }

    /**
     * 构造无效查询计划：
     *   FROM A
     *   PROJECT [a3]   (a3不存在于关系A)
     *
     * 树结构：
     *         Project [a3]
     *                |
     *             Scan(A)
     */
    public static Operator queryInvalid(Catalogue cat) throws Exception {
        Scan a = new Scan(cat.getRelation("A"));
        ArrayList<Attribute> atts = new ArrayList<Attribute>();
        // 此处 "a3" 不存在于关系 A 中，应当抛异常
        atts.add(new Attribute("a3"));
        Project proj = new Project(a, atts);
        return proj;
    }
}
