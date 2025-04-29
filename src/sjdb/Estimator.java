package sjdb;

import java.util.Iterator;
import java.util.List;

public class Estimator implements PlanVisitor {

	public Estimator() {
		// 空构造函数
	}

	/**
	 * 对 Scan 操作符：
	 * 直接将底层 NamedRelation 的统计数据复制到输出 Relation 中。
	 */
	// 这个其实可以不写，因为Scan类的初始化函数里面已经写好了。
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());

		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}

		op.setOutput(output);
	}

	/**
	 * 对 Project 操作符：
	 * 输出元组数不变，且只保留投影列表中指定的属性，
	 * 对于每个属性，将其 distinct 值从子算子输出中复制过来，并限制其不超过输出元组数。
	 */
	public void visit(Project op) {
		// 得到子算子的输出
		Relation input = op.getInput().getOutput();
		// 输出的行数
		int tupleCount = input.getTupleCount();
		// project操作行数不变。
		Relation output = new Relation(tupleCount);

		// 获取要投影的属性列表
		List<Attribute> projAttrs = op.getAttributes();
		// 遍历要保留的每个属性
		for (Attribute projAttr : projAttrs) {
			// 在输入的属性列表中查找与投影属性同名的属性
			Attribute found = null;
			for (Attribute a : input.getAttributes()) {
				if (a.equals(projAttr)) {
					found = a;
					break;
				}
			}
			// 3.2 如果没找到 => 抛出异常
			if (found == null) {
				throw new RuntimeException(new DatabaseException(
						"Attempting to project attribute '" + projAttr.getName() +
								"', but it doesn't exist in the input relation."
				));
			}

			// TODO 要删掉这个逻辑
			// 那个relation里面已经实现了
			// 3.3 如果找到了，就获取 distinct 值并做常规处理
			int distinct = found.getValueCount();
			if (distinct > tupleCount) {
				distinct = tupleCount;
			}
			output.addAttribute(new Attribute(projAttr.getName(), distinct));
		}
		op.setOutput(output);
	}

	/**
	 * 对 Select 操作符：
	 * 根据谓词类型（attr=value 或 attr1=attr2）更新元组数和各属性的 distinct 值。
	 */
	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		int inputTuples = input.getTupleCount();
		Predicate pred = op.getPredicate();

		// 这一部分是计算新的元组数
		int newTuples;
		// 仔细看一下谓词的属性，有三个属性：左属性、右属性和常量
		if (pred.equalsValue()) {
			// 这种情况就是没有右属性，只有左属性和常量
			// 谓词形如 attr = "constant"
			Attribute selAttr = null;
			// 遍历所有的属性
			for (Attribute a : input.getAttributes()) {
				// 找到与谓词属性同名的属性
				if (a.equals(pred.getLeftAttribute())) {
					selAttr = a;
					break;
				}
			}
			// 新增: 若 selAttr==null => 抛异常
			// 这个情况是没有找到同名的属性
			// 其实不会发生，可以不管
			if (selAttr == null) {
				throw new RuntimeException(new DatabaseException(
						"Select error: attribute '" + pred.getLeftAttribute().getName() +
								"' does not exist in input relation!"));
			}
			// distinct(attr) 如果没值就强制 =1, 避免除0
			int d = selAttr.getValueCount();
			// 一般不会出现这种情况
			if (d <= 0){
				d = 1;
			}
			// T（R）/ V（R，attr）
			newTuples = inputTuples / d;
			// 一般不会出现这种情况
			if (newTuples < 1){
				newTuples = 1;
			}
		} else {
			// 这种情况就是有两个属性，没有常量
			// 谓词形如 attr1 = attr2
			Attribute attr1 = null, attr2 = null;
			for (Attribute a : input.getAttributes()) {
				if (a.equals(pred.getLeftAttribute())) {
					attr1 = a;
				}
				if (a.equals(pred.getRightAttribute())) {
					attr2 = a;
				}
			}
			// 新增：若找不到任意一边 => 抛异常
			if (attr1 == null) {
				throw new RuntimeException( new DatabaseException(
						"Select error: left attribute '" + pred.getLeftAttribute().getName() +
								"' does not exist in input relation!"));
			}
			if (attr2 == null) {
				throw new RuntimeException(new DatabaseException(
						"Select error: right attribute '" + pred.getRightAttribute().getName() +
								"' does not exist in input relation!"));
			}
			// 找到两个属性后，获取两个属性的 distinct 值
			int d1 = attr1.getValueCount();
			int d2 = attr2.getValueCount();
			// 假如A有5个不同的值，B有10个不同的值，那么A=B的概率大约是1/10
			int maxd = Math.max(d1, d2);
			// 一般不会出现这种情况，可以抛异常
			if (maxd <= 0){
				maxd = 1;
			}
			newTuples = inputTuples / maxd;
			// 一般不会出现这种情况
			if (newTuples < 1) {
				newTuples = 1;
			}
		}
		// 上面计算出行数，现在找出属性

		Relation output = new Relation(newTuples);

		// 遍历每个属性
		for (Attribute a : input.getAttributes()) {
			// 先初始化默认的值，distinct不能超过新的行数
			// 这个默认值可以理解是未经过选择的列
			int origDistinct = a.getValueCount();
			int newDistinct = Math.min(origDistinct, newTuples);

			// 情况1 对于 attr = constant，选择后的属性 distinct 为 1
			if (pred.equalsValue() && a.equals(pred.getLeftAttribute())) {
				// 因为假如本来取值是1-10，distinct是10，但是现在select要求比如说=2，那么distinct就是1
				newDistinct = 1;
			}
			// 情况2 对于 attr1 = attr2，连接属性的新 distinct 值为两者中较小者与 newTuples 的较小值
			// 假如当前属性是left或者right属性，那么distinct就是两者中较小者与newTuples的较小值
			if (!pred.equalsValue() && (a.equals(pred.getLeftAttribute()) || a.equals(pred.getRightAttribute()))) {
				// 还需要重新找到这两个属性！
				Attribute sel1 = null, sel2 = null;
				for (Attribute b : input.getAttributes()) {
					if (b.equals(pred.getLeftAttribute())) {
						sel1 = b;
					}
					if (b.equals(pred.getRightAttribute())) {
						sel2 = b;
					}
				}
				// 如果没有找到任一属性，则抛出异常
				if (sel1 == null) {
					throw new RuntimeException(new DatabaseException("Select error: left attribute '" +
							pred.getLeftAttribute().getName() + "' not found in input."));
				}
				if (sel2 == null) {
					throw new RuntimeException(new DatabaseException("Select error: right attribute '" +
							pred.getRightAttribute().getName() + "' not found in input."));
				}
				// 得到这两个属性的distinct值
				int dtemp1 = sel1.getValueCount();
				int dtemp2 = sel2.getValueCount();
				// 这个为了防止超过行数，所以多了一层Math.min
				newDistinct = Math.min(Math.min(dtemp1, dtemp2), newTuples);
			}
			output.addAttribute(new Attribute(a.getName(), newDistinct));
		}
		op.setOutput(output);
	}

	/**
	 * 对 Product 操作符（笛卡儿积）：
	 * 输出元组数 = 左子输出元组数 * 右子输出元组数，
	 * 输出属性为左右属性的并集，各属性的 distinct 值不变（但不能超过新元组数）。
	 */
	public void visit(Product op) {
		// 得到两个输入
		// 分别是左子算子的输出和右子算子的输出
		Relation leftRel = op.getLeft().getOutput();
		Relation rightRel = op.getRight().getOutput();
		// 计算笛卡尔积后的行数
		int newTuples = leftRel.getTupleCount() * rightRel.getTupleCount();
		// 行数是两个输入的行数的乘积
		Relation output = new Relation(newTuples);

		// 直接复制左右两个输入的属性
		for (Attribute a : leftRel.getAttributes()) {
			int distinct = a.getValueCount();
			if(distinct > newTuples) {
				distinct = newTuples;
			}
			output.addAttribute(new Attribute(a.getName(), distinct));
		}
		for (Attribute a : rightRel.getAttributes()) {
			int distinct = a.getValueCount();
			if(distinct > newTuples) {
				distinct = newTuples;
			}
			output.addAttribute(new Attribute(a.getName(), distinct));
		}
		op.setOutput(output);
	}

	/**
	 * 对 Join 操作符：
	 * 假设连接谓词为 attr1 = attr2，
	 * 输出元组数 = (左子元组数 * 右子元组数) / max(distinct(left_join), distinct(right_join))，
	 * 并对连接属性，新 distinct 值取 min(左属性 distinct, 右属性 distinct)。
	 */
	// 看上面这个公式，这个和select的非常相似！！！
	// 理解就是： 笛卡尔积得到总个数，乘上A=B的概率，就是最后的个数
	public void visit(Join op) {
		// 拿到左右算子
		Relation leftRel = op.getLeft().getOutput();
		Relation rightRel = op.getRight().getOutput();

		// 获取了本次连接的谓词（形如 attr1=attr2）。
		Predicate pred = op.getPredicate();

		// 先找 leftAttr, rightAttr
		Attribute leftJoin = findAttr(leftRel, pred.getLeftAttribute());
		Attribute rightJoin = findAttr(rightRel, pred.getRightAttribute());

		// 如果找不到，就尝试 swap
		// 说明 (pred.getLeftAttribute()) 其实不在 leftRel，但可能在 rightRel
		// 并且 (pred.getRightAttribute()) 在 leftRel
		if (leftJoin == null || rightJoin == null) {
			// 尝试交换
			Attribute leftJoin2 = findAttr(leftRel, pred.getRightAttribute());
			Attribute rightJoin2 = findAttr(rightRel, pred.getLeftAttribute());
			if (leftJoin2 == null || rightJoin2 == null) {
				// 完全找不到 => 抛异常
				throw new RuntimeException(new DatabaseException(
						"Join error: can't find " + pred.getLeftAttribute() + " or "
								+ pred.getRightAttribute() + " in left/right relation!"
				));
			}
			// 否则 swap
			leftJoin = leftJoin2;
			rightJoin = rightJoin2;
		}

		// 这里 leftJoin, rightJoin 就是对齐好的 pair
		// distinct 计算
		int dLeft = leftJoin.getValueCount();
		int dRight = rightJoin.getValueCount();

		int maxd = Math.max(dLeft, dRight);
		if (maxd == 0) {
			maxd = 1;
		}
		// 根据公式计算出新的行数！
		int newTuples = (leftRel.getTupleCount() * rightRel.getTupleCount()) / maxd;
		if (newTuples < 1){
			newTuples = 1;
		}

		Relation output = new Relation(newTuples);
		// 加入左子关系的属性
		for (Attribute a : leftRel.getAttributes()) {
			int newDistinct = Math.min(a.getValueCount(), newTuples);
			// 只动关联的属性，其他属性不变！
			if (a.equals(leftJoin)) {
				newDistinct = Math.min(newDistinct, Math.min(dLeft, dRight));
			}
			output.addAttribute(new Attribute(a.getName(), newDistinct));
		}
		// 加入右子关系的属性
		for (Attribute a : rightRel.getAttributes()) {
			int newDistinct = Math.min(a.getValueCount(), newTuples);
			if (a.equals(rightJoin)) {
				newDistinct = Math.min(newDistinct, Math.min(dLeft, dRight));
			}
			output.addAttribute(new Attribute(a.getName(), newDistinct));
		}
		op.setOutput(output);
	}

	private Attribute findAttr(Relation rel, Attribute x) {
		for(Attribute a: rel.getAttributes()) {
			if(a.equals(x)) return a;
		}
		return null;
	}
}
