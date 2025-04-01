package sjdb;

import java.util.ArrayList;
import java.util.List;

/**
 * This abstract class represents a binary operator, and is
 * subclassed by Product and Join
 * @author nmg
 *
 */
public abstract class BinaryOperator extends Operator {
	/**
	 * Create a new binary operator
	 */
	public BinaryOperator(Operator left, Operator right) {
		// 先调用父类的构造函数，初始化inputs list
//		public Operator() {
//			this.inputs = new ArrayList<Operator>();
//		}
		super();
		// 然后给这个list添加两个Operator
		this.inputs.add(left);
		this.inputs.add(right);
	}

	/**
	 * Return the left child below this operator in the query plan
	 * @return Left child
	 */
	public Operator getLeft() {
		return this.inputs.get(0);
	}

	/**
	 * Return the right child below this operator in the query plan
	 * @return Right child
	 */
	public Operator getRight() {
		return this.inputs.get(1);
	}

	/* (non-Javadoc)
	 * @see sjdb.Operator#getInputs()
	 */
	@Override
	public List<Operator> getInputs() {
		List<Operator> inputs = new ArrayList<Operator>();
		inputs.addAll(this.inputs);
		return inputs;
	}

	/* (non-Javadoc)
	 * @see sjdb.Operator#getOutput()
	 */
	@Override
	public Relation getOutput() {
		return this.output;
	}
	
	/* (non-Javadoc)
	 * @see sjdb.Operator#accept(sjdb.OperatorVisitor)
	 */
	public void accept(PlanVisitor visitor) {
		super.accept(visitor);
	}
}
