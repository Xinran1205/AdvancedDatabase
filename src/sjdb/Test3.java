package sjdb;

import java.io.*;
import java.util.ArrayList;

public class Test3 {
    public static void main(String[] args) throws Exception {
        // 1. 构建包含三个关系（EMPLOYEE, WORKS_ON, PROJECT）的Catalogue
        Catalogue cat = createCatalogue();

        // 2. 构造canonical查询计划
        Operator plan = queryTeacher(cat);

        // 3. 用Estimator先做一次代价估算、再用Inspector打印
        Estimator estimator = new Estimator();
        plan.accept(estimator);

        Inspector inspector = new Inspector();
        plan.accept(inspector);

        // 4. 调用Optimiser来生成一个优化后计划
        Optimiser optimiser = new Optimiser(cat);
        Operator optPlan = optimiser.optimise(plan);

        // 5. 再估算、再打印
        optPlan.accept(estimator);
        optPlan.accept(inspector);
    }

    /**
     * 创建catalogue:
     * EMPLOYEE(300行): ESSN(300 distinct), LNAME(300), BDATE(300)
     * WORKS_ON(400行): SSN(300 distinct?), PNO(200 distinct?)
     * PROJECT(200行): PNUMBER(200 distinct), PNAME(150 distinct)
     */
    public static Catalogue createCatalogue() {
        Catalogue cat = new Catalogue();

        // EMPLOYEE: 300行
        cat.createRelation("EMPLOYEE", 300);
        // 三个属性
        cat.createAttribute("EMPLOYEE", "ESSN", 300);
        cat.createAttribute("EMPLOYEE", "LNAME", 300);
        cat.createAttribute("EMPLOYEE", "BDATE", 300);

        // WORKS_ON: 400行
        cat.createRelation("WORKS_ON", 400);
        cat.createAttribute("WORKS_ON", "SSN", 300);  // 假设与EMPLOYEE.ESSN有300 distinct
        cat.createAttribute("WORKS_ON", "PNO", 200);

        // PROJECT: 200行
        cat.createRelation("PROJECT", 200);
        cat.createAttribute("PROJECT", "PNUMBER", 200);
        cat.createAttribute("PROJECT", "PNAME", 150);

        return cat;
    }

    /**
     * 构造一个“canonical”查询计划，对应老师的例子：
     * SELECT LNAME
     * FROM EMPLOYEE, WORKS_ON, PROJECT
     * WHERE PNAME="Aquarius"
     *   AND PNUMBER=PNO
     *   AND ESSN=SSN
     *   AND BDATE>"1957-12-31";
     *
     * Canonical form思路：先做3张表的Product，再串接4个Select，最后Project。
     * 树结构(自下而上)大致:
     *
     *          Project [LNAME]
     *                |
     *      Select (BDATE>"1957-12-31")
     *                |
     *      Select (ESSN=SSN)
     *                |
     *      Select (PNUMBER=PNO)
     *                |
     *      Select (PNAME="Aquarius")
     *                |
     *      Product( Product(Scan(EMPLOYEE), Scan(WORKS_ON)), Scan(PROJECT) )
     */
    public static Operator queryTeacher(Catalogue cat) throws Exception {
        // 1. Scan三张表
        Scan scanEmp = new Scan(cat.getRelation("EMPLOYEE"));
        Scan scanWork = new Scan(cat.getRelation("WORKS_ON"));
        Scan scanProj = new Scan(cat.getRelation("PROJECT"));

        // 2. 先构造 Product( EMPLOYEE × WORKS_ON ), 再与 PROJECT 做 Product => 左深
        Product prod1 = new Product(scanEmp, scanWork);
        Product prod2 = new Product(prod1, scanProj);

        // 3. 在最顶层插入一连串的Select
        //    1) SELECT (PNAME="Aquarius")
        //    2) SELECT (PNUMBER=PNO)
        //    3) SELECT (ESSN=SSN)
        //    4) SELECT (BDATE>"1957-12-31")

        // 注意: canonical 里可以是四个Select依次串联，也可以合并，但这里演示分开写
        Select selPNAME = new Select(prod2,
                new Predicate(new Attribute("PNAME"), "Aquarius"));
        Select selPNO   = new Select(selPNAME,
                new Predicate(new Attribute("PNUMBER"), new Attribute("PNO")));
        Select selESSN  = new Select(selPNO,
                new Predicate(new Attribute("ESSN"), new Attribute("SSN")));
        Select selBDATE = new Select(selESSN,
                new Predicate(new Attribute("BDATE"), "1957-12-31") );
        // 这里形如 BDATE="1957-12-31" 其实不完美(> vs =).
        // 只做示例, 你可改"B>someDate".
        // SJDB只支持 = , 所以要假装

        // 4. 最顶层: Project [LNAME]
        ArrayList<Attribute> projList = new ArrayList<Attribute>();
        projList.add(new Attribute("LNAME"));
        Project topProject = new Project(selBDATE, projList);

        return topProject;
    }
}
