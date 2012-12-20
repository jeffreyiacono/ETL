shared_examples "basic etl" do |subject|
  describe '#logger=' do
    let(:etl) { subject.new }

    it 'raises an error if the param does not respond to #log' do
      logger = stub
      logger.stub(:warn)

      lambda {
        etl.logger = logger
      }.should raise_error ArgumentError, /must implement #log/
    end

    it 'raises an error if the param does not respond to #warn' do
      logger = stub
      logger.stub(:log)

      lambda {
        etl.logger = logger
      }.should raise_error ArgumentError, /must implement #warn/
    end

    it 'assigns when the param responds to #log and #warn' do
      logger = stub
      logger.stub(:log)
      logger.stub(:warn)
      etl.logger = logger
      etl.logger.should == logger
    end
  end
end
