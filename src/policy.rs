use std::time::Duration;

/// 定期的に実行される処理のポリシーの一般的な表現。
pub trait IJobPolicy {
    /// 定期的に実行される処理のインターバルを提供する。
    fn interval(&self) -> Duration;

    /// ポリシーに違反しているかどうかの判定を行う。
    fn violate(&self) -> bool;

    /// ポリシーに違反しているかどうかの判定に使う状態を初期状態に戻す。
    ///
    /// 使用例として、リトライ回数を違反状態の判定に使うポリシーにおけるカウントの初期化を行う。
    fn reset(&mut self);

    /// ポリシーに違反しているかどうかの判定に使う状態を更新する。
    ///
    /// 使用例として、リトライ回数を違反状態の判定に使うポリシーにおけるリトライ回数のインクリメントを行う。
    fn update(&mut self);
}

/// デフォルトで提供されるポリシーの実装。
///
/// 内部的にupdateが何回呼ばれたかの状態を持ち、[DefaultJobPolicy]初期化時に与えたlimitを比較し
/// limit以上であれば[IJobPolicy::violate]はtrue, そうでない場合はfalseを返却する。
/// リトライ回数の制御を行う場合などに使用する。
pub struct DefaultJobPolicy {
    interval: Duration,
    limit: usize,
    count: usize,
}

impl DefaultJobPolicy {
    pub fn new(interval: Duration, limit: usize) -> Self {
        Self {
            interval,
            limit,
            count: 0,
        }
    }
}

impl IJobPolicy for DefaultJobPolicy {
    fn interval(&self) -> Duration {
        self.interval
    }

    fn violate(&self) -> bool {
        self.count >= self.limit
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn update(&mut self) {
        self.count += 1;
    }
}
