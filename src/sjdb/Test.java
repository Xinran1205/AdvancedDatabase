package sjdb;

import java.io.*;
import java.util.ArrayList;
import sjdb.DatabaseException;

public class Test {
	private Catalogue catalogue;
	
	public Test() {
	}

	public static void main(String[] args) throws Exception {
		Catalogue catalogue = createCatalogue();
		Inspector inspector = new Inspector();
		Estimator estimator = new Estimator();

		// 调用 plan.accept(estimator)：这是第一次使用访问者，对原始计划进行代价估算。
		// 调用 plan.accept(inspector)：再用另一个访问者来“检查/打印”计划结构和每个算子的输入输出信息。
		Operator plan = query(catalogue); // 构造查询计划
		// 这就让我们把访问者(estimator 或 inspector) 作用到这棵查询计划树(plan 是根节点)上。
		// 根节点就是Project（投影）

		// 这里因为上面query返回的是Project，所以这里的plan就是Project
		// Project是根节点，从Project.accept(estimator)开始，
		// 向上走到Operator.accept(estimator)然后递归调用子节点的accept(estimator)
		plan.accept(estimator);           // 第一次遍历：估算代价
		plan.accept(inspector);           // 第二次遍历：打印计划信息

		// 看看优化过后的结果！
		Optimiser optimiser = new Optimiser(catalogue);
		Operator planopt = optimiser.optimise(plan);
		planopt.accept(estimator);
		planopt.accept(inspector);
	}
	
	public static Catalogue createCatalogue() {
		Catalogue cat = new Catalogue();
		// 表名为A，元组数为100
		cat.createRelation("A", 100);
		// catalogue.createAttribute("A", "a1", 100)
		// 表示在名为 "A" 的关系（表）中添加一个属性 "a1"，并且说明 "a1" 这一列中可以有 100 个不同的值。
		// V(A, a1) = 100
		cat.createAttribute("A", "a1", 100);
		cat.createAttribute("A", "a2", 15);
		cat.createRelation("B", 150);
		cat.createAttribute("B", "b1", 150);
		cat.createAttribute("B", "b2", 100);
		cat.createAttribute("B", "b3", 5);
		
		return cat;
	}


//	query(Catalogue cat) 方法构造了一个查询计划，它依次构建了：
//
//	对表 A 和 B 的 Scan 算子
//
//	通过 Product（笛卡儿积）把 A 和 B 连接
//
//	再通过 Select 对 Product 的结果加上选择谓词（这里就是 a2=b3）
//
//	最后通过 Project 保留指定属性（a2 和 b1）

	// 树结构：
//	          Project
//               |
//	          Select
//               |
//	           Product
//             /   \
//	        Scan    Scan
//		(table A)    (table B)

	public static Operator query(Catalogue cat) throws Exception {
		Scan a = new Scan(cat.getRelation("A"));
		Scan b = new Scan(cat.getRelation("B")); 

		// 可以深入看一下这个构造函数！
		Product p1 = new Product(a, b);
		
		Select s1 = new Select(p1, new Predicate(new Attribute("a2"), new Attribute("b3")));
		
		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("a2"));	
		atts.add(new Attribute("b1"));	

		// 投影操作，根节点
		// 这里可以看出，根节点的参数是一个算子（Select），和一个属性列表
		// 同理select的参数也是一个算子（Product），和一个谓词
		Project plan = new Project(s1, atts);

		// Project，Select，Product，Scan 都是 Operator 的子类
		// 有的继承了 UnaryOperator，有的继承了 BinaryOperator
		return plan;
	}
	
}

